// Those metadata wrap an SFA Ressource Manager.
// http://opensfa.info/doc/opensfa.html
//
// Copyright (C) UPMC Paris Universitas
// Authors:
//   Jordan Auge       <jordan.auge@lip6.fr>
//   Loic Baron        <loic.baron@lip6.fr> 
//   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
//   Amine Larabi      <mohamed.larabi@inria.fr>

enum rm_type {
    "user",
    "slice",
    "authority"
};

//---------------------------------------------------------------------------
// Credentials and GID 
//---------------------------------------------------------------------------

// TODO We could also expose slice_credential, user_credential, authority_credential

class credential {
    const text       user_credential; /**< User credentials. */
    const rm_type    type;            /**< A value among "slice", "user", "authority". */
    const text       hrn;             /**< Human Readable Name of the object related to this gid (ex: ple.upmc.john_doe). */
    const text       certificate;     /**< XML containing X509 certificate + grants that the user has on an object. */

    KEY(user_credential, type, hrn);
};

class gid {
    const text       gid;             /**< A certificate signed by an authority.       */
    const rm_type    type;            /**< A value among "slice", "user", "authority". */
    const text       hrn;             /**< Human Readable Name of the object related to this gid (ex: ple.upmc.john_doe). */

    KEY(gid);
    CAPABILITY(selection, retrieve, join, fullquery);
};

//---------------------------------------------------------------------------
// User
//---------------------------------------------------------------------------

enum role {
    "user",
    "admin",
    "tech",
    "pi"
};

// See sfi.py show ple.upmc.john_doe to expose more fields.

class user {
    const text     first_name;     /**< First name.    */
    const text     last_name;      /**< Last name.     */
    const text     email;          /**< Email address. */
    const text     authority_hrn;
    const gid      gid;
    const text     user_hrn;
    text           public_keys[];
    slice          slices[];       /**< User's slices. */
    const text     phone;          /**< Phone number.  */
    const text     site;
    const role     roles[];        /**< User's roles.  */

    KEY(user_hrn);
    CAPABILITY(retrieve, join, fullquery);
};

//---------------------------------------------------------------------------
// Slice 
//---------------------------------------------------------------------------

class slice {
    const text     slice_hrn;  /**< Slice Human Readable name (ex: ple.upmc.myslice_demo). */
    const text     slice_type; /**< Slice type (ex: "slice"). */
    user           user[];     /**< List of users associated to the slice (see SFA::resolve). */

    KEY(slice_hrn);
    CAPABILITY(retrieve, join, fullquery);
};

//---------------------------------------------------------------------------
// Authority, network 
//---------------------------------------------------------------------------

class authority {
    const text     name;
    const text     abbreviated_name;
    const text     authority_hrn;
    slice          slice[];
    user           user[];

    KEY(authority_hrn);
    CAPABILITY(retrieve, join, fullquery);
};

