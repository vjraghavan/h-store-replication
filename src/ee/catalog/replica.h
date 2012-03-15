/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

/* WARNING: THIS FILE IS AUTO-GENERATED
            DO NOT MODIFY THIS SOURCE
            ALL CHANGES MUST BE MADE IN THE CATALOG GENERATOR */

#ifndef CATALOG_REPLICA_H_
#define CATALOG_REPLICA_H_

#include <string>
#include "catalogtype.h"
#include "catalogmap.h"

namespace catalog {

class Host;
/**
 * A replica for a site
 */
class Replica : public CatalogType {
    friend class Catalog;
    friend class CatalogMap<Replica>;

protected:
    Replica(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);

    int32_t m_id;
    int32_t m_primarySiteId;
    CatalogType* m_host;
    int32_t m_messenger_port;
    int32_t m_proc_port;

    virtual void update();

    virtual CatalogType * addChild(const std::string &collectionName, const std::string &name);
    virtual CatalogType * getChild(const std::string &collectionName, const std::string &childName) const;
    virtual void removeChild(const std::string &collectionName, const std::string &childName);

public:
    /** GETTER: Replica Id */
    int32_t id() const;
    /** GETTER: Which is the primary site ID of this replica? */
    int32_t primarySiteId() const;
    /** GETTER: Which host does the replica belong to? */
    const Host * host() const;
    /** GETTER: Port used by HStoreCoordinator */
    int32_t messenger_port() const;
    /** GETTER: Port used by VoltProcedureListener */
    int32_t proc_port() const;
};

} // namespace catalog

#endif //  CATALOG_REPLICA_H_
