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

#include <cassert>
#include "replica.h"
#include "catalog.h"
#include "host.h"

using namespace catalog;
using namespace std;

Replica::Replica(Catalog *catalog, CatalogType *parent, const string &path, const string &name)
: CatalogType(catalog, parent, path, name)
{
    CatalogValue value;
    m_fields["id"] = value;
    m_fields["primarySiteId"] = value;
    m_fields["host"] = value;
    m_fields["messenger_port"] = value;
    m_fields["proc_port"] = value;
}

void Replica::update() {
    m_id = m_fields["id"].intValue;
    m_primarySiteId = m_fields["primarySiteId"].intValue;
    m_host = m_fields["host"].typeValue;
    m_messenger_port = m_fields["messenger_port"].intValue;
    m_proc_port = m_fields["proc_port"].intValue;
}

CatalogType * Replica::addChild(const std::string &collectionName, const std::string &childName) {
    return NULL;
}

CatalogType * Replica::getChild(const std::string &collectionName, const std::string &childName) const {
    return NULL;
}

void Replica::removeChild(const std::string &collectionName, const std::string &childName) {
    assert (m_childCollections.find(collectionName) != m_childCollections.end());
}

int32_t Replica::id() const {
    return m_id;
}

int32_t Replica::primarySiteId() const {
    return m_primarySiteId;
}

const Host * Replica::host() const {
    return dynamic_cast<Host*>(m_host);
}

int32_t Replica::messenger_port() const {
    return m_messenger_port;
}

int32_t Replica::proc_port() const {
    return m_proc_port;
}

