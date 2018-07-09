#!/bin/bash


psql -d postgres -U kitestring -w -f /media/sf_DetectorStressQuantification/Modules/developmental/ResetDetectorstressDB.sql

psql -d detectorstress -U kitestring -w -f /media/sf_DetectorStressQuantification/Modules/SchemaSetup.sql
