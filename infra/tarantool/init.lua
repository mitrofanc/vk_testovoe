local config = require('config')
local APP_USER = 'kv_app'
local COUNT_FUNCTION_NAME = 'kv_count'
local app_password = os.getenv('TARANTOOL_PASSWORD')

if app_password == nil or app_password == '' then
    error('TARANTOOL_PASSWORD must be set for kv_app user')
end

local cfg = config:get()
local info = config:info('v2')

if info.status ~= 'ready' and info.status ~= 'check_warnings' then
    error(('Tarantool YAML configuration was not applied successfully: %s'):format(info.status))
end

local function safe_revoke(username, privileges, object_type, object_name)
    local ok, err = pcall(box.schema.user.revoke, username, privileges, object_type, object_name)

    if ok then
        return
    end

    local message = tostring(err)
    if message:match('ER_PRIV_NOT_GRANTED') or message:match('does not have') then
        return
    end

    error(err)
end

local kv = box.space.kv
if kv == nil then
    kv = box.schema.space.create('kv', {
        engine = 'memtx',
        if_not_exists = true
    })
end

kv:format({
    {name = 'key', type = 'string'},
    {name = 'value', type = 'varbinary', is_nullable = true}
})

kv:create_index('primary', {
    type = 'TREE',
    parts = {
        {field = 'key', type = 'string'}
    },
    if_not_exists = true
})

rawset(_G, COUNT_FUNCTION_NAME, function(space_name)
    local target_space_name = space_name or 'kv'
    local target_space = box.space[target_space_name]

    if target_space == nil then
        error(('Space %s does not exist'):format(target_space_name))
    end

    return target_space:len()
end)

box.schema.func.create(COUNT_FUNCTION_NAME, {if_not_exists = true})

safe_revoke('guest', 'read,write,execute', 'universe', nil)
box.schema.user.disable('guest')

box.schema.user.create(APP_USER, {password = app_password, if_not_exists = true})
box.schema.user.passwd(APP_USER, app_password)
box.schema.user.enable(APP_USER)
box.schema.user.grant(APP_USER, 'read,write', 'space', 'kv', {if_not_exists = true})
box.schema.user.grant(APP_USER, 'execute', 'function', COUNT_FUNCTION_NAME, {if_not_exists = true})

return {
    config = cfg,
    status = info.status
}
