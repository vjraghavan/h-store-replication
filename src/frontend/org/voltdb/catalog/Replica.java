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

package org.voltdb.catalog;

/**
 * A replica for a site
 */
public class Replica extends CatalogType {

    int m_id;
    int m_primarySiteId;
    int m_messenger_port;
    int m_proc_port;

    void setBaseValues(Catalog catalog, CatalogType parent, String path, String name) {
        super.setBaseValues(catalog, parent, path, name);
        this.addField("id", m_id);
        this.addField("primarySiteId", m_primarySiteId);
        this.addField("host", null);
        this.addField("messenger_port", m_messenger_port);
        this.addField("proc_port", m_proc_port);
    }

    public void update() {
        m_id = (Integer) m_fields.get("id");
        m_primarySiteId = (Integer) m_fields.get("primarySiteId");
        m_messenger_port = (Integer) m_fields.get("messenger_port");
        m_proc_port = (Integer) m_fields.get("proc_port");
    }

    /** GETTER: Replica Id */
    public int getId() {
        return m_id;
    }

    /** GETTER: Which is the primary site ID of this replica? */
    public int getPrimarysiteid() {
        return m_primarySiteId;
    }

    /** GETTER: Which host does the replica belong to? */
    public Host getHost() {
        Object o = getField("host");
        if (o instanceof UnresolvedInfo) {
            UnresolvedInfo ui = (UnresolvedInfo) o;
            Host retval = (Host) m_catalog.getItemForRef(ui.path);
            assert(retval != null);
            m_fields.put("host", retval);
            return retval;
        }
        return (Host) o;
    }

    /** GETTER: Port used by HStoreCoordinator */
    public int getMessenger_port() {
        return m_messenger_port;
    }

    /** GETTER: Port used by VoltProcedureListener */
    public int getProc_port() {
        return m_proc_port;
    }

    /** SETTER: Replica Id */
    public void setId(int value) {
        m_id = value; m_fields.put("id", value);
    }

    /** SETTER: Which is the primary site ID of this replica? */
    public void setPrimarysiteid(int value) {
        m_primarySiteId = value; m_fields.put("primarySiteId", value);
    }

    /** SETTER: Which host does the replica belong to? */
    public void setHost(Host value) {
        m_fields.put("host", value);
    }

    /** SETTER: Port used by HStoreCoordinator */
    public void setMessenger_port(int value) {
        m_messenger_port = value; m_fields.put("messenger_port", value);
    }

    /** SETTER: Port used by VoltProcedureListener */
    public void setProc_port(int value) {
        m_proc_port = value; m_fields.put("proc_port", value);
    }

}
