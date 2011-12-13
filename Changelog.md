# Ordasity Changelog
### Version History

#### Current: Ordasity 0.2.0
**Changes Since Previous Release:**

– Resolved a distributed race condition which can occur when using Ordasity's "handoff" capability at the very end of the process.

– Resolved a case in which a node may fail to recognize that it has completed the claiming process after accepting handoff of a work unit from another node.

#### Previous Releases:
###### Ordasity 0.1.9 ::
– Resolved a case in which a node in an Ordasity cluster could find itself in a confused state after losing connection to a Zookeeper node, but successfully re-establishing the connection prior to the expiration of the Zookeeper session timeout.

– Migrated node state information stored in Zookeeper from a flat string to structured JSON.

Nodes running a previous version of Ordasity will see nodes running Ordasity 0.1.9+, but will view them in a "fallback" mode and operate under the safe assumption that the unrecognized nodes might not attempt to claim work. This safeguard ensures that all work units will remain claimed during the upgrade, but may result in 0.1.8 nodes claiming more than their fair share while it's in progress. As such, this release is safe for a rolling upgrade with no special treatment required.


###### Ordasity 0.1.8 ::
Initial release