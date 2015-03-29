## Reliability Guarantees

This chapter discusses the reliability features that you can expect out of Onyx. These features are tuneable and require the developer to make trade-offs.

### At Least Once Delivery

Onyx guarantees that each message that comes out of an input task will be fully processed. If a peer disconnects from Onyx while processing a segment, that segment is replayed from the input task from which it came.