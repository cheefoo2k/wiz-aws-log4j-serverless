#!/bin/sh

aws dynamodb put-item --table-name publisher_data --item file://item.json --return-consumed-capacity TOTAL --return-item-collection-metrics SIZE
