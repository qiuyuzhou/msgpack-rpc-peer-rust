# Share bounded execution across requests and notifications

Request and notification handlers share a bounded execution budget and may complete out of arrival order. Overloaded notifications are dropped and counted; notification failures and handler panics are exposed through bounded diagnostics without parameters. Under panic unwinding, request-handler panics produce a generic remote error; applications needing ordering or acknowledged delivery must provide those semantics through application methods.
