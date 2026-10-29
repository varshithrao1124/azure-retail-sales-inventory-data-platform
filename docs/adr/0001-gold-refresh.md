# Gold is rebuilt from silver

Silver is incremental: a batch watermark plus MERGE. Gold is a full overwrite each successful run.

As-of SCD joins and the inventory rollforward both need a consistent cut of every dimension version and every movement. Rebuilding gold avoids a second incremental state machine for late facts that change an earlier order date. The sample fact is tens of rows locally and, at the medium generator setting, a few thousand orders. That is not a clickstream. Partitioning by order date still lets a later incremental design overwrite one day if the volume grows.

A failed run that stops after silver (`--fail-after silver`) leaves the watermark committed and gold absent. The next run skips the files and rebuilds gold.
