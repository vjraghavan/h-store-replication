package edu.brown.catalog;

import java.util.Set;

import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Host;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Replica;
import org.voltdb.catalog.Site;

//(kowshik) A wrapper over Catalog Site to help perform transaction replication
/**** FOR TRANSACTION REPLICATION ***/
public class ReplicaWrapperSite extends Site {

	private final Site site;
	private final Replica replica;

	public ReplicaWrapperSite(Site s, Replica r) {
		this.site = s;
		this.replica = r;
	}

	@Override
	public int getProc_port() {
		return replica.getProc_port();
	}

	@Override
	public int getMessenger_port() {
		return replica.getMessenger_port();
	}

	@Override
	public void setMessenger_port(int value) {
		replica.setMessenger_port(value);
	}

	@Override
	public void setProc_port(int value) {
		replica.setProc_port(value);
	}

	@Override
	public void update() {
		site.update();
	}

	@Override
	public int getId() {
		return site.getId();
	}

	@Override
	public Host getHost() {
		return site.getHost();
	}

	@Override
	public CatalogMap<Partition> getPartitions() {
		return site.getPartitions();
	}

	@Override
	public CatalogMap<Replica> getReplicas() {
		return site.getReplicas();
	}

	@Override
	public boolean getIsup() {
		return site.getIsup();
	}

	@Override
	public void setId(int value) {
		site.setId(value);
	}

	@Override
	public void setHost(Host value) {
		site.setHost(value);
	}

	@Override
	public void setIsup(boolean value) {
		site.setIsup(value);
	}

	@Override
	public String getPath() {
		return site.getPath();
	}

	@Override
	public String getTypeName() {
		return site.getTypeName();
	}

	@Override
	public String getName() {
		return site.getName();
	}

	@Override
	public <T extends CatalogType> T getParent() {
		return site.getParent();
	}

	@Override
	public Catalog getCatalog() {
		return site.getCatalog();
	}

	@Override
	public int getRelativeIndex() {
		return site.getRelativeIndex();
	}

	@Override
	public int getNodeVersion() {
		return site.getNodeVersion();
	}

	@Override
	public int getSubTreeVersion() {
		return site.getSubTreeVersion();
	}

	@Override
	public Set<String> getFields() {
		return site.getFields();
	}

	@Override
	public Object getField(String field) {
		return site.getField(field);
	}

	@Override
	public Set<String> getChildFields() {
		return site.getChildFields();
	}

	@Override
	public CatalogType getChild(String collectionName, String childName) {
		return site.getChild(collectionName, childName);
	}

	@Override
	public CatalogMap<? extends CatalogType> getChildren(String collectionName) {
		return site.getChildren(collectionName);
	}

	@Override
	public void set(String field, String value) {
		site.set(field, value);
	}

	@Override
	public int compareTo(CatalogType o) {
		return site.compareTo(o);
	}

	@Override
	public String toString() {
		return site.toString();
	}

	@Override
	public String fullName() {
		return site.fullName();
	}

	@Override
	public boolean equals(Object obj) {
		return site.equals(obj);
	}

	@Override
	public int hashCode() {
		return site.hashCode();
	}
} // END CLASS

