-- geo.lua
-- Geographic bounding box based search routines.
-- Nate Folkman and Jay Ridgeway, 2009

--[[

ttserver -ext geo.lua "casket.tct#idx=x:dec#idx=y:dec"

tcrmgr ext localhost put 1 "lat,40.709092,lng,-73.921412"
tcrmgr ext localhost put 2 "lat,40.738007,lng,-73.883625"
tcrmgr ext localhost put 3 "lat,40.680177,lng,-73.959199"
tcrmgr ext localhost put 4 "lat,40.739843,lng,-74.003047"
tcrmgr ext localhost put 5 "lat,40.700616,lng,-73.917979"
tcrmgr list -pv -sep ", "  localhost

tcrmgr ext localhost distance "40.738007,-73.883625" "40.700616,-73.917979"

tcrmgr ext localhost geosearch "40.700616,-73.917979" 50

--]]


-- namespace for geo routines
geo = {}


--
-- constants
--


-- kilometers in a mile (not used)
geo.kminamile = 1.609344

-- ~radius of the earth in miles
geo.radius = 3958.75587

-- miles per degree of lattitude
geo.latDistance = 69.169144

-- table of miles between successive longitudinal degrees 
geo.longDistance = {69.1691, 69.1586, 69.1270, 69.0743, 69.0006, 68.9059, 68.7902, 68.6536, 68.4960, 68.3175, 68.1183, 67.8983, 67.6576, 67.3963, 67.1145, 66.8122, 66.4896, 66.1467, 65.7837, 65.4006, 64.9976, 64.5749, 64.1324, 63.6704, 63.1890, 62.6884, 62.1687, 61.6300, 61.0726, 60.4965, 59.9020, 59.2893, 58.6586, 58.0099, 57.3436, 56.6598, 55.9588, 55.2407, 54.5058, 53.7543, 52.9864, 52.2023, 51.4024, 50.5868, 49.7558, 48.9097, 48.0486, 47.1729, 46.2829, 45.3787, 44.4607, 43.5292, 42.5844, 41.6267, 40.6563, 39.6735, 38.6786, 37.6719, 36.6537, 35.6244, 34.5842, 33.5335, 32.4726, 31.4018, 30.3214, 29.2318, 28.1333, 27.0262, 25.9109, 24.7877, 23.6570, 22.5190, 21.3742, 20.2229, 19.0654, 17.9021, 16.7333, 15.5595, 14.3809, 13.1979, 12.0109, 10.8203, 9.6264, 8.4295, 7.2301, 6.0284, 4.8249, 3.6200, 2.4139, 1.2072, 0.0000, 1.2072, 2.4139, 3.6200, 4.8249, 6.0284, 7.2301, 8.4295, 9.6264, 10.8203, 12.0109, 13.1979, 14.3809, 15.5595, 16.7333, 17.9021, 19.0654, 20.2229, 21.3742, 22.5190, 23.6570, 24.7877, 25.9109, 27.0262, 28.1333, 29.2318, 30.3214, 31.4018, 32.4726, 33.5335, 34.5842, 35.6244, 36.6537, 37.6719, 38.6786, 39.6735, 40.6563, 41.6267, 42.5844, 43.5292, 44.4607, 45.3787, 46.2829, 47.1729, 48.0486, 48.9097, 49.7558, 50.5868, 51.4024, 52.2023, 52.9864, 53.7543, 54.5058, 55.2407, 55.9588, 56.6598, 57.3436, 58.0099, 58.6586, 59.2893, 59.9020, 60.4965, 61.0726, 61.6300, 62.1687, 62.6884, 63.1890, 63.6704, 64.1324, 64.5749, 64.9976, 65.4006, 65.7837, 66.1467, 66.4896, 66.8122, 67.1145, 67.3963, 67.6576, 67.8983, 68.1183, 68.3175, 68.4960, 68.6536, 68.7902, 68.9059, 69.0006, 69.0743, 69.1270, 69.1586, 69.1691}


--
-- functions
--


-- convert from float to natural number
function geo.tonatural(pt)
  return math.floor(pt * 10000 + 1800000)
end


-- miles between point a and point b
function geo.distance(lat1, lng1, lat2, lng2)
  local rlat = lat1*math.pi/180;
  local rlng = lng1*math.pi/180;
  local rlat2 = lat2*math.pi/180;
  local rlng2 = lng2*math.pi/180;

  if (rlat == rlat2 and rlng == rlng2) then
    return 0
  else
    -- Spherical Law of Cosines
    return geo.radius*math.acos(math.sin(rlat)*math.sin(rlat2)
      +math.cos(rlng-rlng2)*math.cos(rlat)*math.cos(rlat2))
  end
end


-- returns bounding box vertices
function geo.box(lat, lng, miles)
  local lngD = miles/geo.longDistance[math.abs(string.format("%.0f", lat))+1]
  local latD = miles/geo.latDistance

  local llat = (lat+latD > 180 and 180 - lat+latD or lat+latD)
  local llng = (lng+lngD > 180 and 180 - lng+lngD or lng+lngD)
  local ulat = (lat-latD < -180 and 180 + lat-latD or lat-latD)
  local ulng = (lng-lngD < -180 and 180 + lng-lngD or lng-lngD)
  return ulat, ulng, llat, llng
end


--
-- tcrmgr routines
--


- wrapper for geo.distance
function distance(a, b)
  a = _split(a, ',')
  b = _split(b, ',')
  return geo.distance(a[1], a[2], b[1], b[2])
end


-- puts a new entry into the db
function put(id, data)
  local t = {}
  local cols = _split(data, ',')
  
  for i = 1, #cols, 2 do
    t[cols[i]] = cols[i+1]
  end  
  if t["lat"] == nil or t["lng"] == nil then
    return "lat/lng required"
  end
  if t["x"] ~= nil or t["y"] ~= nil or t["distance"] ~= nil then
    return "x/y/distance are reserved"
  end
  
  local x = geo.tonatural(t["lat"])
  local y = geo.tonatural(t["lng"])
  table.insert(cols, "x")
  table.insert(cols, x)
  table.insert(cols, "y")
  table.insert(cols, y)
  
  return _put(id, table.concat(cols, '\0'))
end


- tcrmgr geo search
function geosearch(pt, miles)
  pt = _split(pt, ",")
  local X = pt[1]
  local Y = pt[2]  
  local lx,ly,ux,uy = geo.box(X, Y, miles)
  
  local args = {}
  table.insert(args, "addcond\0x\0NUMGT\0" .. geo.tonatural(lx))
  table.insert(args, "addcond\0x\0NUMLT\0" .. geo.tonatural(ux))
  table.insert(args, "addcond\0y\0NUMGT\0" .. geo.tonatural(ly))
  table.insert(args, "addcond\0y\0NUMLT\0" .. geo.tonatural(uy))
  table.insert(args, "get")
  
  local res = _misc("search", args)
  if res == nil then
    return ""
  end
  
  table.sort(res, function (a,b)
    local sa = _split(a)
    local sb = _split(b)
    local ta = {}
    local tb = {}
    for i=1, #sa, 2 do ta[sa[i]] = sa[i+1] end
    for i=1, #sb, 2 do tb[sb[i]] = sb[i+1] end
    
    local da = geo.distance(X, Y, ta["lat"], ta["lng"])
    local db = geo.distance(X, Y, tb["lat"], tb["lng"])
    return (da <     db)
  end)

  local val = {}
  for i=1, #res do
    local ary = {}
    local row = _split(res[i])
    table.remove(row, 1)
    table.insert(row, 1, "id")
    for i=1, #row, 2 do
      if row[i] == nil then row[i] = "id" end
      ary[row[i]] = row[i+1]
    end
    table.insert(row, "distance")
    table.insert(row, geo.distance(X, Y, ary["lat"], ary["lng"]))    
    table.insert(val, table.concat(row,","))
  end

  return table.concat(val, "\n")
end
