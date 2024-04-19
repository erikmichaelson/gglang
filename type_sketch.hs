// a Parser takes the raw gglang (or sgl I haven't decided yet)
// and makes a Dataflow Type that can be compiled or run straight-up
Parser :: String -> Dataflow

// a runtime takes a dataflow graph as an input
// and it visualizes it and makes it pretty
// (and run fast)
// I don't know if this is the correct Typing for
// Haskell... maybe
Runtime :: Dataflow | Binary -> Runtime

// here you have to run it and then it
// tracks usage and database reads / writes patterns
// and it optimizes configurations / which type of
// database would make it visualize fastest
Optimize :: Runtime -> Binary

Compile :: Dataflow -> Binary

ReadBinary :: () -> Binary
