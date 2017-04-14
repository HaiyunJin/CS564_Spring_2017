// can't write a query? state why here

All queries can be done within the functions provided by sqlite3 v3.18.0.

The only trouble I met is that nested queries are not allowed in the FROM
clause. This leads me to use the WITH functionality of sqlites.

