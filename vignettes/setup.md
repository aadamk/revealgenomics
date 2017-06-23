# Security

The API assumes the following:
- SciDB EE (enterprise edition) is installed. 
(Install instructions are [here](https://downloads.paradigm4.com/enterprise/16.9/SciDB-Installation-v16.9.pdf).
Contact support@paradigm4.com for login credentials)
- Data can be stored in up to three security namespaces -- 
'public', 'collaboration' and 'clinical'. ('public' is available 
by default; and at least one more namespace must exist)

Follow the steps below to set up the security settings for the API to work:

1. Turn on security for SciDB by following instructions at
https://paradigm4.atlassian.net/wiki/display/ESD169/More+Info
2. Set up the root user (instructions at 
https://paradigm4.atlassian.net/wiki/display/ESD169/Setting+up+the+Root+User)

At this point, you should be able to run the following:
```sh
iquery --auth-file ~/.scidb_root_auth -aq "show_user();"
iquery --auth-file ~/.scidb_root_auth -aq "list()"
iquery --auth-file ~/.scidb_root_auth -aq "list('libraries')"
```

3. Use the following trick to bypass the `auth-file` parameter:

Open a config file:
```sh
vi ~/.config/scidb/iquery.conf
# Paste the following there
############################
{
"auth-file":"/home/scidb/.scidb_root_auth"
}
############################
```

At this point, you should be able to run:

```sh
iquery -aq "list('libraries')"
```
