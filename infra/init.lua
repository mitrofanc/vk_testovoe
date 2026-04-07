local config = require('config')

local cfg = config:get()
local info = config:info('v2')

if info.status ~= 'ready' and info.status ~= 'check_warnings' then
    error(('Tarantool YAML configuration was not applied successfully: %s'):format(info.status))
end

if box.space.kv == nil then
    local kv = box.schema.space.create('kv', {
        engine = 'memtx',
        if_not_exists = true
    })

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
end

box.schema.user.grant('guest', 'read', 'universe', nil, {if_not_exists = true})
box.schema.user.grant('guest', 'write', 'universe', nil, {if_not_exists = true})
box.schema.user.grant('guest', 'execute', 'universe', nil, {if_not_exists = true})

return {
    config = cfg,
    status = info.status
}
