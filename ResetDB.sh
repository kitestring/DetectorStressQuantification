#!/bin/bash


psql -d postgres -U kitestring -w -f /media/sf_DetectorStressQuantification/DB_Setup/ResetDetectorstressDB.sql

psql -d detectorstress -U kitestring -w -f /media/sf_DetectorStressQuantification/DB_Setup/SchemaSetup.sql
