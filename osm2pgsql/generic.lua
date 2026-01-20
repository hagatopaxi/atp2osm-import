local srid = 9794

local tables = {}

tables.points = osm2pgsql.define_node_table('points', {
    { column = 'tags', type = 'jsonb' },
    { column = 'geom', type = 'point', projection = srid, not_null = true },
    { column = 'version', type = 'int' },
})

tables.polygons = osm2pgsql.define_area_table('polygons', {
    { column = 'tags', type = 'jsonb' },
    { column = 'geom', type = 'geometry', projection = srid, not_null = true },
    { column = 'version', type = 'int' },
})

-- Based on tags wiki list, that removes every POI which are definitely not places
-- https://wiki.openstreetmap.org/wiki/Map_features
local function is_definitely_not_a_place(tags)
    if tags["aerialway"] then return true end
    if tags["barrier"] then return true end	
    if tags["boundary"] then return true end
    if tags["emergency"] then return true end
    if tags["highway"] then return true end
    if tags["lifeguard"] then return true end
    if tags["geological"] then return true end
    if tags["military"] then return true end
    if tags["place"] then return true end
    if tags["power"] then return true end
    if tags["telecom"] then return true end
    if tags["water"] then return true end

    if tags["building"] == 'industrial' then return true end	
    if tags["building"] == 'warehouse' then return true end	
    if tags["building"] == 'bridge' then return true end	
    if tags["building"] == 'digester' then return true end	
    if tags["building"] == 'tech_cab' then return true end	
    if tags["building"] == 'transformer_tower' then return true end	
    if tags["building"] == 'water_tower' then return true end	
    if tags["building"] == 'storage_tank' then return true end	

    if tags["landuse"] == 'industrial' then return true end	
    if tags["landuse"] == 'construction' then return true end	
    if tags["landuse"] == 'aquaculture' then return true end	
    if tags["landuse"] == 'farmyard' then return true end	
    if tags["landuse"] == 'flowerbed' then return true end	
    if tags["landuse"] == 'farmyard' then return true end	
    if tags["landuse"] == 'depot' then return true end	
    if tags["landuse"] == 'quarry' then return true end	
    if tags["landuse"] == 'railway' then return true end	

    if tags["railway"] and tags["railway"] ~= 'halt' then return true end
    if tags["railway"] and tags["railway"] ~= 'stop_position' then return true end
    if tags["railway"] and tags["railway"] ~= 'stop' then return true end
    if tags["railway"] and tags["railway"] ~= 'station' then return true end
    if tags["railway"] and tags["railway"] ~= 'platform' then return true end
    if tags["railway"] and tags["railway"] ~= 'subway_entrance' then return true end
    if tags["railway"] and tags["railway"] ~= 'tram_stop' then return true end

    return false
end 

function osm2pgsql.process_node(object)
    local tags = object.tags
    if is_definitely_not_a_place(tags) then return end

    tables.points:insert({
        tags = object.tags,
        geom = object:as_point(),
        version = object.version,
    })
end

function osm2pgsql.process_relation(object)
    local tags = object.tags
    if is_definitely_not_a_place(tags) then return end
    
    local relation_type = object:grab_tag('type')

    if relation_type == 'multipolygon' then
        tables.polygons:insert({
            tags = object.tags,
            geom = object:as_multipolygon(),
            version = object.version,
        })
    end
end
