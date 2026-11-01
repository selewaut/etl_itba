-- dimension de la estación de sensado
CREATE TABLE IF NOT EXISTS geolocation0 -- nivel sensor
(
id serial NOT NULL PRIMARY KEY, -- autonumerico
lat double precision not null,
lon double precision not null,
datum text not null,
countyfk int not null,
unique(lat, lon, datum)
);
CREATE TABLE IF NOT EXISTS geolocation1 -- nivel county
(
id serial NOT NULL PRIMARY KEY, -- autonumerico
countyname text not null,
statefk int not null,
-- etc etc
unique (countyname, statefk)
);
CREATE TABLE IF NOT EXISTS geolocation2 -- nivel state
(
id serial NOT NULL PRIMARY KEY, -- autonumerico
statename text not null,
countryfk int not null,
-- etc etc
unique (statename, countryfk)
);


CREATE TABLE IF NOT EXISTS geolocation3 -- nivel country
(
id serial NOT NULL PRIMARY KEY, -- autonumerico
countryname text not null,
-- etc etc
unique (countryname)
);
ALTER TABLE geolocation0 DROP CONSTRAINT IF EXISTS geo0fk;
ALTER TABLE geolocation0 ADD CONSTRAINT geo0fk
foreign key(countyfk) references geolocation1;
ALTER TABLE geolocation1 DROP CONSTRAINT IF EXISTS geo1fk;
ALTER TABLE geolocation1 ADD CONSTRAINT geo1fk
foreign key(statefk) references geolocation2;
ALTER TABLE geolocation2 DROP CONSTRAINT IF EXISTS geo2fk;
ALTER TABLE geolocation2 ADD CONSTRAINT geo2fk
foreign key(countryfk) references geolocation3;