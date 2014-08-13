# Create database and tables in PostgreSQL
# ----------------------------------------
#
# $ psql -U postgres
# postgres=# CREATE DATABASE onelab;
#
# $ psql -U postgres -d onelab
#
# The gateway assumes this aggregate is available:
# CREATE AGGREGATE array_accum (anyelement)
# (
#     sfunc = array_append,
#     stype = anyarray,
#     initcond = '{}'
# );
#
# DROP TABLE IF EXISTS authority_data;
# CREATE TABLE authority_data (
#     authority_hrn text PRIMARY KEY,
#     name text,
#     address text,
#     postcode text,
#     city text,
#     country text,
#     longitude text,
#     latitude text,
#     description text,
#     onelab_membership text,
#     url text,
#     legal text,
#     scientific text,
#     tech text,
#     enabled boolean
# );
#
# DROP TABLE IF EXISTS slice_data;
# CREATE TABLE slice_data (
#     slice_urn text PRIMARY KEY,
#
#     url text,
#     purpose text
# );
#
# DROP TABLE IF EXISTS user_data;
# CREATE TABLE user_data (
#     user_hrn text PRIMARY KEY,
#
#     first_name text,
#     last_name text
# );
#
#
# NOTE: A table should have a primary key, and links are based on references to other tables
#
#
# Add a PostgreSQL gateway called 'onelabdata'
# --------------------------------------------
#
# manifold-add-platform onelab-data "OneLab SFA additional data" postgresql none '{"db_user": "postgres", "db_name": "onelab"}' 0 
# manifold-enable-platform onelab-data
# manifold-shell -z local
#
#
# Metadata
# --------
#
# The following class declarations are necessary so that the keys are
# references to the SFA classes. Without these declarations, the gateway relies
# on REFERENCES, which in our case does not exist in the database.

class authority_data {
    authority authority_hrn;

    string name;
    string address;
    string postcode;
    string city;
    string country;
    string longitude;
    string latitude;
    string description;
    string onelab_membership;
    string url;
    string legal;
    string scientific;
    string tech;
    bool enabled;

    KEY(authority_hrn);
    CAPABILITY(join, selection, projection);
};

class slice_data {
    slice slice_urn;   /**< Slice Unique Resource Name */

	string url;
	string purpose;

    KEY(slice_urn);
    CAPABILITY(join, selection, projection);
};

class user_data {
    user user_hrn;

	string first_name;
	string last_name;

    KEY(user_hrn);
    CAPABILITY(join, selection, projection);
};
