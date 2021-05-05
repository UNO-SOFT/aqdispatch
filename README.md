# AQDispatch
Dispatch RPC over The Big Red's Advanced Queue (`DBMS_AQ`).

You give the request type (needs a Name and a Payload),
and the response type (needs an Errmsg and a Payload) to New,
and Dispatcher receives the messages, and calls the given do
function for each messages.

The output of that function is sent back as answer.

This is a dispatcher, which splits input over the NAME of the
task, and manages separate disk queues for each NAME.
