local dataset_size = tonumber(os.getenv('LOAD_DATASET_SIZE') or '')
local prefix = os.getenv('LOAD_DATASET_PREFIX') or 'full:'
local progress_step = tonumber(os.getenv('LOAD_DATASET_PROGRESS_STEP') or '250000')
local varbinary = require('varbinary')

if dataset_size == nil or dataset_size <= 0 then
    error('LOAD_DATASET_SIZE must be set to a positive integer')
end

dofile('/opt/tarantool/init.lua')

local kv = box.space.kv
if kv == nil then
    error('Space kv was not initialized')
end

if kv:len() ~= 0 then
    error('Expected an empty kv space when preparing a preseeded dataset')
end

local function indexed_key(index)
    return ('%s%07d'):format(prefix, index)
end

local function seed_value(index)
    if index == 0 then
        return box.NULL
    end

    if index == 1 then
        return varbinary.new('')
    end

    return varbinary.new(('value-%d'):format(index))
end

for index = 0, dataset_size - 1 do
    kv:replace({indexed_key(index), seed_value(index)})

    if progress_step > 0 and ((index + 1) % progress_step == 0 or index + 1 == dataset_size) then
        print(('Prepared %d/%d records'):format(index + 1, dataset_size))
    end
end

if kv:len() ~= dataset_size then
    error(('Prepared dataset size mismatch: expected %d actual %d'):format(dataset_size, kv:len()))
end

box.snapshot()

print(('Dataset preparation completed: size=%d prefix=%s'):format(dataset_size, prefix))
os.exit(0)
