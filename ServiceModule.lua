local Services = {
	Session = {}
}
local DataStoreService = game:GetService('DataStoreService')
local CollectionService = game:GetService('CollectionService')
local HttpService = game:GetService('HttpService')
local RunService = game:GetService('RunService')
local Players = game:GetService('Players')
local MessagingService = game:GetService('MessagingService')
local ReplicatedStorage = game:GetService('ReplicatedStorage')

local eventListen = {}
local asyncQueue = {}
local queueMutex = false
local config = {
	max = 30,
	retry = 0.3,
	versionAsync = 'v1.0',
	logLevel = 'Info'
}

function dispatchEvent(event, ...)
	if eventListen[event] then
		for _, listener in ipairs(eventListen[event]) do
			coroutine.wrap(listener)(...)
		end
	end
end

function logMessage(level, message)
	if level == 'Error' or config.logLevel == 'Debug' or (config.logLevel == 'Info' and level ~= 'Debug') then
		local levelMessage = string.format('[ServiceModule] [%s] %s', level, message)
		if level == 'Error' then
			error(levelMessage)
		elseif level == 'Warn' then
			warn(levelMessage)
		else
			print(levelMessage)
		end
	end
end

function enqueueAsync(func)
	while queueMutex do task.wait(0.01) end
	queueMutex = true
	table.insert(asyncQueue, func)
	queueMutex = false
end

local function processQueue()
	while true do
		if #asyncQueue > 0 then
			local func
			while queueMutex do task.wait(0.01) end
			queueMutex = true
			func = table.remove(asyncQueue, 1)
			queueMutex = false
			local success, err = pcall(func)
			if not success then
				logMessage('Error', string.format("Queuing process error: %s", err))
				dispatchEvent('queueError', err)
			else
				dispatchEvent('queueProcessed', func)
			end
		end
		task.wait(0.1)
	end
end

function retryAsync(func)
	local retries = 0
	while retries < config.max do
		local success, result = pcall(func)
		if success then return result else
			retries += 1
			logMessage('Warn', string.format('Retrying ... (%d/%d) %s',  retries, config.max, result))
		end
	end
	error(string.format("Failed after %d retries", config.max))
end

coroutine.wrap(processQueue)()

function Services.DataStore(dataName: string?, dataScope: string?, option: Instance?)
	dataName = dataName or 'PlayerDataStore'
	local DataStore = DataStoreService:GetDataStore(dataName, dataScope, option)
	local events = {}
	events.__index = events
	local ActiveSession = {}
	local ConflictTimer = {}
	
	local function IsLocked(player: Player)
		return ActiveSession[player] ~= nil
	end
	
	local function LockSession(player: Player)
		if IsLocked(player) then
			error('Players data is locked by another session')
		end
		ActiveSession[player] = os.time()
	end
	
	local function UnlockSession(player: Player)
		ActiveSession[player] = nil
		ConflictTimer[player] = nil
	end
	
	local function HandleSession(player: Player)
		local conflict = ConflictTimer[player] or os.time()
		ConflictTimer[player] = conflict
		if os.time() - conflict >= 60 then
			player:Kick('Session is already active in another game. Please rejoin')
		end
	end
	
	function events.new()
		local self = {
			DataStore = DataStore,
			DataStoreService = DataStoreService,
		}
		setmetatable(self, events)
		return self
	end

	function events:GetAsync(player: Player, warnMessage: string?)
		if IsLocked(player) then
			HandleSession(player)
			return nil
		end
		LockSession(player)
		local playerName = Players:GetNameFromUserIdAsync(player)
		local success, data = pcall(function()
			return retryAsync(function()
				return self.DataStore:GetAsync(player)
			end)
		end)
		if not success then player:Kick(warnMessage) else
			logMessage('Warn', `{playerName}, your data was successfully restored`)
			UnlockSession(player)
		end
		local decode
		if data then
			decode = HttpService:JSONDecode(data)
		end
		return decode
	end

	function events:GetBudgetRequests(BudgetType: Enum.DataStoreRequestType)
		local current = self.DataStoreService:GetRequestBudgetForRequestType(BudgetType)
		while current < 1 do
			task.wait(5)
			current = self.DataStoreService:GetRequestBudgetForRequestType(BudgetType)
		end
	end

	function events:SetAsync(player: Player, data: any, canBindData: boolean?, userids: {any}?, options: DataStoreSetOptions?)
		if IsLocked(player) then
			HandleSession(player)
			return false
		end
		LockSession(player)
		local encode = HttpService:JSONEncode(data)
		local success, err
		repeat
			if not canBindData then self:GetBudgetRequests(Enum.DataStoreRequestType.SetIncrementAsync) end
			success, err = pcall(function()
				return retryAsync(function()
					return self.DataStore:SetAsync(player, encode, userids, options)
				end)
			end)
		until success
		local playerName = Players:GetNameFromUserIdAsync(player)
		if not success then
			logMessage('Warn', `{playerName} data was not successfully saved, {err}`)
		else
			logMessage('Warn', `{playerName} data was successfully saved`)
			UnlockSession(player)
		end
	end

	function events:UpdateAsync(player: Player, transform: (oldData: any) -> (), canBindData: boolean?)
		if IsLocked(player) then
			HandleSession(player)
			return false
		end
		LockSession(player)
		local success, err
		repeat
			if not canBindData then self:GetBudgetRequests(Enum.DataStoreRequestType.UpdateAsync) end
			success, err = pcall(function()
				return enqueueAsync(function()
					retryAsync(function()
						return self.DataStore:UpdateAsync(player, function(oldData)
							oldData = oldData and HttpService:JSONDecode(oldData)
							local newData = transform(oldData)
							return HttpService:JSONEncode(newData)
						end)
					end)
				end)
			end)
		until success
		local playerName = Players:GetNameFromUserIdAsync(player)
		if not success then
			logMessage('Warn', `Failed to Update data to: {playerName}`)
		else
			UnlockSession(player)
			logMessage('Warn', `Successfully saved data to: {playerName}`)
		end
	end

	function events:RemoveAsync(player: Player)
		return self.DataStore:RemoveAsync(player)
	end

	function events:BindDataOnUpdate(canBind: boolean?)
		canBind = canBind or false
		if canBind then
			game:BindToClose(function()
				if RunService:IsServer() then return task.wait(2) end
				local bind = Instance.new('BindableEvent')
				local allPlayers = Players:GetPlayers()
				local allCurrent = #allPlayers
				for _, player in pairs(allPlayers) do
					task.spawn(function()
						self:UpdateAsync(player, true)
						allCurrent -= 1
						if allCurrent <= 0 then bind:Fire() end
					end)
				end
				bind.Event:Wait()
			end)
		end
	end

	function events:BindDataOnSet(canBind: boolean?)
		canBind = canBind or false
		if canBind then
			game:BindToClose(function()
				if RunService:IsServer() then return task.wait(2) end
				local bind = Instance.new('BindableEvent')
				local allPlayers = Players:GetPlayers()
				local allCurrent = #allPlayers
				for _, player in pairs(allPlayers) do
					task.spawn(function()
						self:SetAsync(player, true)
						allCurrent -= 1
						if allCurrent <= 0 then bind:Fire() end
					end)
				end
				bind.Event:Wait()
			end)
		end
	end
	
	return events.new()
end

function Services.Players()
	local events = {}
	events.__index = events

	function events.new()
		local self = {
			Players = Players
		}
		setmetatable(self, events)
		return self
	end

	function events:PlayerAdded(callBack: (player: Player) -> RBXScriptSignal)
		return self.Players.PlayerAdded:Connect(callBack)
	end

	function events:PlayerRemoving(callBack: (player: Player) -> RBXScriptSignal)
		return self.Players.PlayerRemoving:Connect(callBack)
	end

	function events:GetPlayers(): {Players}
		return self.Players:GetPlayers()
	end

	function events:GetAllPlayers(): number
		local players = self:GetPlayers()
		return #players
	end

	return events.new()
end

function Services.Collection()
	local events = {}
	events.__index = events

	function events.new()
		local self = {
			CollectionService = CollectionService
		}
		setmetatable(self, events)
		return self
	end

	function events:AddTag(objectName: string, tag: any)
		if not self.CollectionService:HasTag(objectName, tag) then
			self.CollectionService:AddTag(objectName, tag)
		end
	end

	function events:RemoteTag(objectName: string, tag: any)
		if self.CollectionService:HasTag(objectName, tag) then
			self.CollectionService:RemoveTag(objectName, tag)
		end
	end

	function events:GetTagged(tag: string)
		return self.CollectionService:GetTagged(tag)
	end

	function events:ConnectTags(tag: string, callBack: (object: Instance) -> ())
		self.CollectionService:GetInstanceAddedSignal(tag):Connect(callBack)
		self.CollectionService:GetInstanceRemovedSignal(tag):Connect(function(object)
			logMessage('Info', string.format(`Object: {'%s'} removed from tag: {'%s'}`, object.Name, tag))
		end)
	end

	return events.new()
end

function Services.Number()
	local events = {}
	events.__index = events
	local first = {"", "U","D","T","Qd","Qn","Sx","Sp","Oc","No"}
	local second = {"", "De","Vt","Tg","qg","Qg","sg","Sg","Og","Ng"}
	local third = {'', 'Ce'}

	function events.eq(val1: number, val2: number)
		return val1 == val2
	end

	function events.me(val1: number, val2: number)
		return val1 > val2
	end

	function events.le(val1: number, val2: number)
		return val1 < val2
	end

	function events.meeq(val1: number, val2: number)
		return (val1 > val2) or (val1 == val2)
	end

	function events.leeq(val1: number, val2: number)
		return (val1 < val2) or (val1 == val2)
	end

	function events.floor(val1)
		return math.floor(val1 * 100 + 0.001) / 100
	end

	function events.add(val1, val2, canRound: boolean?)
		local value = val1+val2
		if canRound then	return events.floor(value)	end
		return value
	end

	function events.div(val1, val2, canRound: boolean?)
		local value = val1/val2
		if canRound then	return events.floor(value)end
		return value
	end

	function events.mul(val1, val2, canRound: boolean?)
		local value = val1*val2
		if canRound then	return events.floor(value) end
		return value
	end

	function events.sub(val1, val2, canRound: boolean?)
		val1 = val1 - val2
		if val1 <= 0 then return 0 end
		if canRound then	return events.floor(val1) end
		return val1
	end

	function events.log(value, canRound: boolean?)
		value = math.log(value)
		if canRound then	return events.floor(value) end
		return value
	end

	function events.logx(val1, val2, canRound: boolean?)
		local value = math.log(val1, val2)
		if canRound then	return events.floor(value) end
		return value
	end

	function events.log10(val1, canRound: boolean?)
		return events.logx(val1, 10, canRound)
	end

	function events.pow(val1, val2, canRound: boolean?)
		val1 = val1^val2
		if canRound then return events.floor(val1) end return val1
	end

	function events.clamp(value: number, min: number, max: number)
		if events.me(min, max) then min, max = max, min end
		if events.le(value, min) then return min elseif events.me(value, max) then return max end
		return value
	end

	function events.min(val1, val2)
		return val1 < val2 and val1 or val2
	end

	function events.max(val1, val2)
		return val1 > val2 and val1 or val2
	end

	function events.mod(val1, val2, canRound: boolean?)
		local value = val1 % val2
		if canRound then	return events.floor(value) end
		return value
	end

	function events.factorial(val1)
		if val1 == 0 then return 1 end
		local result = 1
		for i = 2, val1 do
			result = result * i
		end
		return result
	end

	function events.Comma(value)
		if value >= 1e3 then
			value = math.floor(value)
			local format = tostring(value)
			format = format:reverse():gsub('(%d%d%d)', '%1,'):reverse()
			if format:sub(1, 1) == ',' then format = format:sub(2) end return format
		end
		return value
	end

	function events.toTable(value)
		if value == 0 then return {0, 0} end
		local exp = math.floor(math.log10(math.abs(value)))
		return {value / 10^exp, exp}
	end

	function events.toNumber(value)
		return (value[1] * (10^value[2]))
	end

	function events.toNotation(value, canRound: boolean?)
		local toTable = events.toTable(value)
		local man, exp = toTable[1], toTable[2]
		if canRound then	return events.floor(man) .. 'e' .. exp end
		return man .. 'e' .. exp
	end

	local function suffixPart(index)
		local hun = math.floor(index/100)
		index = index%100
		local ten, one = math.floor(index/10), index % 10
		return (first[one+1] or '') ..(second[ten+1] or '') .. (third[hun+1] or '')
	end

	function events.short(value)
		local toTable = events.toTable(value)
		local exp, man = toTable[2], toTable[1]
		if exp < 3 then return math.floor(value * 100 + 0.001)/100 end
		local ind = math.floor(exp/3)-1
		if ind > 101 then return 'inf' end
		local rm = exp%3
		man = math.floor(man*10^rm * 100 + 0.001) / 100
		if ind == 0 then
			return man .. 'k'
		elseif ind == 1 then
			return man ..'m'
		elseif ind == 2 then
			return man .. 'b'
		end
		return man .. suffixPart(ind)
	end

	function events.shortE(value: number, canRound: boolean?, canNotation: number?): 'Notation will automatic preset but if u want one smaller do it as 1e3'
		canNotation = canNotation or 1e6
		if math.abs(value) >= canNotation then return events.toNotation(value, canRound):gsub('nane','')	end
		return events.short(value)
	end

	function events.maxBuy(c, b, r, k)
		local en = events
		local max = en.div(math.log(en.add(en.div(en.mul(c , en.sub(r , 1)) , en.mul(b , en.pow(r,k))) , 1)) , en.log(r))
		local cost =  en.mul(b , en.div(en.mul(en.pow(r,k) , en.sub(en.pow(r,max) , 1)), en.sub(r , 1)))
		local nextCost = en.mul(b, en.pow(r,max))
		return max, cost, nextCost
	end

	function events.CorrectTime(value: number)
		local days = math.floor(value / 86400)
		local hours = math.floor((value % 86400) / 3600)
		local minutes = math.floor((value % 3600) / 60)
		local seconds = value % 60
		local result = ""
		local function appendTime(unit, label)
			if unit > 0 then	result = result .. string.format(':%d%s', unit, label)	end
		end
		if days > 0 then
			result = string.format('%dd', days) appendTime(hours, 'h')	appendTime(minutes, 'm') appendTime(seconds, 's') 
		elseif hours > 0 then
			result = string.format('%dh', hours)	appendTime(minutes, 'm') appendTime(seconds, 's')
		elseif minutes > 0 then
			result = string.format('%dm', minutes) 	appendTime(seconds, 's')
		else
			result = string.format('%ds', seconds)
		end
		return result
	end

	function events.percent(part, total, canRound: boolean?)
		local value = (part / total) * 100
		if canRound then	return events.floor(value) end
		if value < 0.001 then return '0%' end
		return value .. '%'
	end

	function events.Changed(value, callBack: (property: string) -> ())
		value.Changed:Connect(callBack)
	end

	function events.Concat(value, canRound: boolean?, canNotation: number?)
		canNotation = canNotation or 1e6
		if value >= canNotation then return events.shortE(value, canRound, canNotation) end
		return events.Comma(value)
	end

	function events.lbencode(value)
		local toTable = events.toTable(value)
		local man, exp = toTable[1], toTable[2]
		if man == 0 then return 4e18 end
		local mode = 0
		if man < 0 then
			mode = 1
		elseif man > 0 then
			mode = 2
		end
		local val = mode * 1e18
		if mode == 2 then
			val += (exp * 1e14) + (math.log10(math.abs(man))*1e13)
		elseif mode == 1 then
			val += (exp * 1e14) + (math.log10(math.abs(man))*1e13)
			val = 1e17 - val
		end
		return val
	end

	function events.lbdecode(value)
		if value == 4e18 then return {0,0} end
		local mode = math.floor(value/1e18)
		if mode == 1 then
			local v = 1e18 - value
			local exp = math.floor(v/1e14)
			local man = 10^((v%1e14)/1e13)
			return events.toNumber({-man, exp})
		elseif mode == 2 then
			local v = value - 2e18
			local exp = math.floor(v/1e14)
			local man = 10^((v%1e14)/1e13)
			return events.toNumber({man, exp})
		end
	end

	function events.GetValue(valueName, Player: Player)
		local class = {}
		for _, values in pairs(Player:GetDescendants()) do
			if values:IsA('ValueBase') and values.Name == valueName and values.Name ~= 'BoundKeys' then
				class.Name = values.Name :: string
				class.Value = values.Value :: number
				class.Parent = values.Parent :: Instance
			end
		end
		return class
	end

	function events.Roman(value)
		local toRoman = {1000, 900, 500, 400, 100, 90, 50, 40, 10, 9, 5, 4, 1}
		local suffix = {"M", "CM", "D", "CD", "C", "XC", "L", "XL", "X", "IX", "V", "IV", "I"}
		local rom = ''
		for i, val in ipairs(toRoman) do
			while events.meeq(value, val) do
				rom = rom .. suffix[i]
				value = value - val
			end
		end
		return rom
	end

	function events.getCurrentData(value, oldValue)
		local new = value
		if oldValue then
			local old = events.lbdecode(oldValue)
			new = events.max(new, old)
		end
		return events.lbencode(new)
	end

	return events
end

function Services.Messaging()
	local events = {}
	events.__index = events

	function events.new()
		local self = {
			MessagingService = MessagingService
		}
		setmetatable(self, events)
		return self
	end

	function events:PublishMessage(topic: string, message: any)
		local success, err = pcall(function()
			self.MessagingService:PublishAsync(topic, message)
		end)
		if not success then
			logMessage('Error', `Failed to publish message to topic {topic}: {err}`)
		end
	end

	function events:SubscribeToTopic(topic: string, callBack: (message: any) ->())
		local success, err = pcall(function()
			return self.MessagingService:SubscribeAsync(topic, callBack)
		end)
		if not success then
			logMessage('Error', `Failed to subscribe to topic {topic}: {err}`)
		end
	end

	return events.new()
end

function Services.Remotes()
	local events = {}
	events.__index = events

	function events.new(Replicated: ReplicatedStorage)
		local self = {
			RemoteEvents = {},
			RemoteFunction = {}
		}
		for _, event in pairs(Replicated:GetDescendants()) do
			if event:IsA('RemoteEvent') then
				self.RemoteEvents[event.Name] = event
			elseif event:IsA('RemoteFunction') then
				self.RemoteFunction[event.Name] = event
			end
		end
		setmetatable(self, events)
		return self
	end

	function events:GetRemoteEvent(remoteName: string)
		local event = self.RemoteEvents[remoteName]
		return self.RemoteEvents[remoteName]
	end

	function events:OnServerEvent(remoteName: string, func: (player: Player, ...any) -> ())
		local event = self:GetRemoteEvent(remoteName)
		return event.OnServerEvent:Connect(func)
	end

	function events:GetRemoteFunction(remoteName: string)
		return self.RemoteFunction[remoteName]
	end

	function events:OnServerInvoke(remoteName: string, func: (player: Player, ...any) -> ())
		local event = self:GetRemoteFunction(remoteName)
		event.OnServerInvoke = func
	end
	
	function events:FireClient(remoteName: string, player: Player, ...: any)
		local event = self:GetRemoteEvent(remoteName):: RemoteEvent
		event:FireClient(player, ...)
	end
	
	function events:FireAllClients<T...>(remoteName: string, ...: T...)
		local event = self:GetRemoteEvent(remoteName)
		event:FireAllClients(...)
	end

	return events.new(ReplicatedStorage)
end

function Services.GetData()
	local events = {}
	
	function events.data(player: Player)
		local session = Services.Session[player]
		if not session then return end
		return session
	end
	
	return events
end

return Services