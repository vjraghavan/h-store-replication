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
 * A physical execution context for the system
 */
public class Site extends CatalogType {

    int m_id;
    CatalogMap<Partition> m_partitions;
    CatalogMap<Replica> m_replicas;
    boolean m_isUp;
    int m_messenger_port;
    int m_proc_port;

    void setBaseValues(Catalog catalog, CatalogType parent, String path, String name) {
        super.setBaseValues(catalog, parent, path, name);
        this.addField("id", m_id);
        this.addField("host", null);
        m_partitions = new CatalogMap<Partition>(catalog, this, path + "/" + "partitions", Partition.class);
        m_childCollections.put("partitions", m_partitions);
        m_replicas = new CatalogMap<Replica>(catalog, this, path + "/" + "replicas", Replica.class);
        m_childCollections.put("replicas", m_replicas);
        this.addField("isUp", m_isUp);
        this.addField("messenger_port", m_messenger_port);
        this.addField("proc_port", m_proc_port);
    }

    public void update() {
        m_id = (Integer) m_fields.get("id");
        m_isUp = (Boolean) m_fields.get("isUp");
        m_messenger_port = (Integer) m_fields.get("messenger_port");
        m_proc_port = (Integer) m_fields.get("proc_port");
    }

    /** GETTER: Site Id */
    public int getId() {
        return m_id;
    }

    /** GETTER: Which host does the site belong to? */
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

    /** GETTER: Which logical data partition does this host process? */
    public CatalogMap<Partition> getPartitions() {
        return m_partitions;
    }

    /** GETTER: What are the replicas of this site? */
    public CatalogMap<Replica> getReplicas() {
        return m_replicas;
    }

    /** GETTER: Is the site up? */
    public boolean getIsup() {
        return m_isUp;
    }

    /** GETTER: Port used by HStoreCoordinator */
    public int getMessenger_port() {
        return m_messenger_port;
    }

    /** GETTER: Port used by VoltProcedureListener */
    public int getProc_port() {
        return m_proc_port;
    }

    /** SETTER: Site Id */
    public void setId(int value) {
        m_id = value; m_fields.put("id", value);
    }

    /** SETTER: Which host does the site belong to? */
    public void setHost(Host value) {
        m_fields.put("host", value);
    }

    /** SETTER: Is the site up? */
    public void setIsup(boolean value) {
        m_isUp = value; m_fields.put("isUp", value);
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
