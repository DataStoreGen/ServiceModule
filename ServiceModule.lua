-- want new features dm me @bela_dimitrescu1940 on discord
local Services = {}
local DataStoreService = game:GetService('DataStoreService')
local CollectionService = game:GetService('CollectionService')
local HttpService = game:GetService('HttpService')
local RunService = game:GetService('RunService')
local Players = game:GetService('Players')
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
		local levelMessage = string.format('[DataStoreModule] [%s] %s', level, message)
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
	
	function events.new()
		local self = {
			DataStore = DataStore,
			DataStoreService = DataStoreService,
		}
		setmetatable(self, events)
		return self
	end
	
	function events:GetAsync(player: Player, warnMessage: string?)
		local playerName = Players:GetNameFromUserIdAsync(player)
		local success, data = pcall(function()
			return retryAsync(function()
				return self.DataStore:GetAsync(player)
			end)
		end)
		if not success then player:Kick(warnMessage) else
			logMessage('Warn', `{playerName}, your data was successfully restored`)
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
		local playerName = Players:GetNameFromUserIdAsync(player)
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
		if not success then
			logMessage('Warn', `{playerName} data was not successfully saved, {err}`)
		else
			logMessage('Warn', `{playerName} data was successfully saved`)
		end
	end
	
	function events:UpdateAsync(player: Player, transform: (oldData: any) -> (), canBindData: boolean?)
		local success, err
		repeat
			if not canBindData then self:BindData(Enum.DataStoreRequestType.UpdateAsync) end
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
		if not success then
			logMessage('Warn', `Failed to Update data to: {player.Name}`)
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
	
	return events.new()
end

return Services