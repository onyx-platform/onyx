Goals:

* Serialize keys only once to improve amount of serialization i.e. fast deduplication? Create a lookup table to ensure re-use. Pass key dictionary down occasionally.
* Only deserialize keys or values on use.
* Only reserialize keys or values if they change.
* Re-use bytes/values on re-serialization to reduce memory allocation.
