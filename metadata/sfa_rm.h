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
    const string       user_credential; /**< User credentials. */
    const rm_type    type;            /**< A value among "slice", "user", "authority". */
    const string       hrn;             /**< Human Readable Name of the object related to this gid (ex: ple.upmc.john_doe). */
    const string       certificate;     /**< XML containing X509 certificate + grants that the user has on an object. */

    KEY(user_credential, type, hrn);
};

class gid {
    const string       gid;             /**< A certificate signed by an authority.       */
    const rm_type    type;            /**< A value among "slice", "user", "authority". */
    const string       hrn;             /**< Human Readable Name of the object related to this gid (ex: ple.upmc.john_doe). */

    KEY(gid);
    CAPABILITY(selection, retrieve, join, fullquery);
};

//---------------------------------------------------------------------------
// User
//---------------------------------------------------------------------------

//enum role {
//    "user",
//    "admin",
//    "tech",
//    "pi"
//};

class user {
    /* SFA FIELDS */
    const string user_hrn;              /**< User Human Readable Name   */
    const string user_urn;              /**< User Unique Resource Name  */
    const string user_type;             /**< Object type == user        */
    const string user_email;            /**< Email address              */
    const string user_gid;              /**< User GID                   */
    const authority parent_authority;   /**< User's authority           */
    const string keys;                  /**< user's keys                */
    slice slices[];                     /**< User's slices              */
    authority pi_authorities[];         /**< User's PI authorities      */

    /* TESTBED FIELDS */

    /* UNCLASSIFIED */
    const string user_first_name;       /**< First name                 */
    const string user_last_name;        /**< Last name                  */
    const string user_phone;            /**< Phone number               */
    const string user_enabled;
    /* roles, site ? */

    KEY(user_hrn);
    CAPABILITY(retrieve, join, fullquery);
};


//---------------------------------------------------------------------------
// Slice 
//---------------------------------------------------------------------------

class slice {
    /* SFA FIELDS */
    const text slice_urn;               /**< Slice Human Readable name  */
    const text slice_hrn;               /**< Slice Human Readable name  */
    const text slice_type;              /**< Object type == slice       */
    user        users[];                /**< Slice's users              */
    user        pi_users[];             /**< Slice's PIs                */

    /* TESTBED FIELDS */

    /* UNCLASSIFIED */
    const text slice_description;
    const text created;
    const text slice_expires;
    const text slice_last_updated;
    const text nodes;
    const text slice_url;
    const authority parent_authority;

    KEY(slice_urn);
    CAPABILITY(retrieve, join, fullquery);
};

//---------------------------------------------------------------------------
// Authority, network 
//---------------------------------------------------------------------------

class authority {
    /* SFA FIELDS */
    const string authority_hrn;         /**< Authority Human Readable Name  */
    const authority parent_authority;   /**< Authority's parent authority   */
    slice      slices[];                /**< Authority's slices             */
    user       users[];                 /**< Authority's users              */
    user       pi_users[];              /**< Authority's PI users           */

    /* TESTBED FIELDS */
    const string name;
    const string abbreviated_name;

    KEY(authority_hrn);
    CAPABILITY(retrieve, join, fullquery);
};

