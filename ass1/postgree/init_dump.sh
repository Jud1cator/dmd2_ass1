#!/bin/sh
psql -U $POSTGRES_USER -d $POSTGRES_DB -f /postgree/restore.sql
