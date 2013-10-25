# -*- coding: utf-8 -*-
#
# See manifold/models/platform.py
#
# A Platform represents a source of data. A Platform is related
# to a Gateway, which wrap this source of data in the Manifold
# framework. For instance, TDMI is a Platform using the PostgreSQL
# Gateway.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

import json
from sqlalchemy                     import Column, Integer, String, Boolean, Enum
from types                          import StringTypes

from manifold.core.query            import Query
from manifold.models                import Base
from manifold.util.storage          import DBStorage
from manifold.util.type             import accepts, returns 

class Platform(Base):
    platform_id          = Column(Integer, doc = "Platform identifier", primary_key = True)
    platform             = Column(String,  doc = "Platform name", unique = True)
    platform_longname    = Column(String,  doc = "Platform long name")
    platform_description = Column(String,  doc = "Platform description")
    platform_url         = Column(String,  doc = "Platform URL")
    deleted              = Column(Boolean, doc = "Platform has been deleted", default = False)
    disabled             = Column(Boolean, doc = "Platform has been disabled", default = False)
    status               = Column(String,  doc = "Platform status")
    status_updated       = Column(Integer, doc = "Platform last check")
    platform_has_agents  = Column(Boolean, doc = "Platform has agents", default = False)
    first                = Column(Integer, doc = "First timestamp, in seconds since UNIX epoch")
    last                 = Column(Integer, doc = "Last timestamp, in seconds since UNIX epoch")
    gateway_type         = Column(String,  doc = "Type of the gateway to use to connect to this platform")
    auth_type            = Column(Enum('none', 'default', 'user', 'reference', 'managed'), default = 'default')
    config               = Column(String,  doc = "Default configuration (serialized in JSON)")

#DEPRECATED|    def get_object(self):
#DEPRECATED|        config = json.loads(self.config) if self.config else {}
#DEPRECATED|        return Object(self.platform, self.gateway_type, config, self.auth_type)

    @returns(dict)
    def get_config(self):
        """
        Retrieve the configuration related to this Platform.
        Returns:
            The corresponding dictionnary. It may be an empty dictionnary.
        """
        platform_name = self.platform
        if platform_name == "dummy":
            platform_config = self.gateway_config
        else:
            platform_config_json = self.config
            platform_config = json.loads(platform_config_json) if platform_config_json else dict()
        return platform_config

    @staticmethod
    @returns(int)
    def get_platform_id(platform_name):
        """
        (Internal use)
        This crappy method is used since we do not exploit Manifold
        to perform queries on the Manifold storage, so we manually
        retrieve platform_id.
        Args:
            platform_name: a String instance. 
        Returns:
            The platform ID related to a Platform.
        """
        ret = db.query(User.user_id).filter(Platform.platform == platform_name).one()
        return ret[0]

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The '%r' representation of this Platform.
        """
        return self.platform

    @returns(StringTypes)
    def __str__(self):
        """
        Returns:
            The '%s' representation of this Platform.
        """
        return "Platform<%s (%s)>" % (self.platform, self.get_config())

    @returns(dict)
    def get_user_config(self, user):
        """
        Retrieve the dictionnary storing the user information related 
        this Platform.
        Args:
            user: A User instance.
        Returns:
            The corresponding user configuration in a dictionnary.
            It may be an empty dictionnary. 
        """
        platform_name = self.platform

        if not self.auth_type:
            Log.warning("'auth_type' is not set in self = %s" % self)
            return None

        # XXX platforms might have multiple auth types (like pam)
        # XXX we should refer to storage

        if self.auth_type in ["none", "default"]:
            user_config = dict() 

        # For default, take myslice account
        elif self.auth_type == "user":
            # User account information
            accounts = [account for account in user.accounts if account.platform.platform == platform_name]
            if accounts:
                #raise Exception, "No such account"
                account = accounts[0]
                user_config = json.loads(account.config)

                if account.auth_type == "reference":
                    ref_platform_name = user_config["reference_platform"]

                    #ref_platform = db.query(Platform).filter(Platform.platform == ref_platform).one()
                    ref_platform  = DBStorage.execute(Query().get("platform").filter_by("platform", "==", ref_platform_name), format = "object")
                    if not ref_platform:
                        raise Exception, "Reference platform not found: %s" % ref_platform_name
                    ref_platform, = ref_platform

                    ref_accounts = [account for account in user.accounts if account.platform.platform == ref_platform.platform]
                    if not ref_accounts:
                        raise Exception, "Reference account does not exist"
                    ref_account = ref_accounts[0]

                    user_config = json.loads(ref_account.config)
            else:
                user_config = None
        else:
            raise ValueError("This 'auth_type' not supported: %s" % self.auth_type)

        return user_config
 
