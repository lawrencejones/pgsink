# Generic Sink utilities

Sinks should be spoken about in abstract, as managing the flow of data from
the changelog into a sink is complex, and we prefer to create reliable
abstractions once that can be used everywhere.

With this focus, the `generic` package provides composable sink constructors.
