insert into platform 
    (platform, platform_longname, gateway_type, gateway_conf, config, disabled) 
VALUES (
    'ple',
    'PlanetLab Europe',
    'SFA',
    '{"auth": "ple.upmc", "user": "ple.upmc.slicebrowser", "sm": "http://www.planet-lab.eu:12346/", "registry": "http://www.planet-lab.eu:12345/", "user_private_key": "/var/myslice/myslice.pkey"}',
    '{}',
    0
);

insert into platform 
    (platform, platform_longname, gateway_type, gateway_conf, config, disabled) 
VALUES (
    'plc',
    'PlanetLab Central',
    'SFA',
    '{"auth": "ple.upmc", "user": "ple.upmc.slicebrowser", "sm": "http://www.planet-lab.org:12346/", "registry": "http://www.planet-lab.org:12345/", "user_private_key": "/var/myslice/myslice.pkey"}',
    '{"auth_ref": "ple"}',
    1
);

insert into platform 
    (platform, platform_longname, gateway_type, gateway_conf, config, disabled) 
VALUES (
    'omf',
    'NITOS',
    'SFA',
    '{"auth": "ple.upmc", "user": "ple.upmc.slicebrowser", "sm": "http://sfa-omf.pl.sophia.inria.fr:12346/", "registry": "http://sfa-omf.pl.sophia.inria.fr:12345/", "user_private_key": "/var/myslice/myslice.pkey"}',
    '{"auth_ref": "ple"}',
    0
);

insert into platform (platform, platform_longname, gateway_type, gateway_conf) VALUES ('tophat', 'TopHat', 'XMLRPC', '{"url": "https://api.top-hat.info/API/"}');
insert into platform (platform, platform_longname, gateway_type, gateway_conf) VALUES ('myslice', 'MySlice', 'XMLRPC', '{"url": "https://api.myslice.info/API/"}');
insert into platform (platform, platform_longname, gateway_type, gateway_conf) VALUES ('maxmind', 'MaxMind GeoLite City', 'MaxMind', '');

insert into user (user_id, email, password) VALUES (1, 'demo', '$1$dd0facf3$bwT92WWK8VG5Mwr3HT/0g/');

insert into account (user_id, platform_id, auth_type, config) VALUES (1, 1, 'user', '{"user_hrn": "ple.upmc.jordan_auge"}');
insert into account (user_id, platform_id, auth_type, config) VALUES (1, 2, 'reference', '{"reference_platform": "ple"}');
insert into account (user_id, platform_id, auth_type, config) VALUES (1, 3, 'reference', '{"reference_platform": "ple"}');