 # Pre-partition baseline — Railway demo

Captured: 2026-05-14T02:28:29Z
Database: railway (schema tm_vault)

## Aggregate totals

```
 tenants | resources | jobs | snapshots | items  | partitions 
---------+-----------+------+-----------+--------+------------
       1 |       141 |    4 |        78 | 236114 |          0
(1 row)

```

## Resources by type
```
      type       | n  |      storage_mib       
-----------------+----+------------------------
 USER_CHATS      |  9 |  7126.1882867813110352
 USER_ONEDRIVE   |  9 |  4958.8234834671020508
 ONEDRIVE        | 14 |  2781.0698843002319336
 USER_MAIL       |  9 |  1177.0746183395385742
 MAILBOX         | 11 |  1177.0745897293090820
 USER_CALENDAR   |  9 |     5.6546325683593750
 ENTRA_USER      | 38 | 0.05779743194580078125
 USER_CONTACTS   |  9 | 0.00375747680664062500
 SHARED_MAILBOX  |  1 | 0.00000000000000000000
 ENTRA_GROUP     |  4 | 0.00000000000000000000
 TEAMS_CHANNEL   | 11 | 0.00000000000000000000
 SHAREPOINT_SITE |  9 | 0.00000000000000000000
 ROOM_MAILBOX    |  1 | 0.00000000000000000000
 M365_GROUP      |  7 | 0.00000000000000000000
(14 rows)

```

## Backup jobs — recent 20 (with wall-time + bytes)
```
ERROR:  column "started_at" does not exist
LINE 1: ...ch_path=tm_vault,public; SELECT id, type, status, started_at...
                                                             ^
```

## Snapshots — last 30 completed (per-resource wall-time + bytes)
```
                  id                  | resource_type |         display_name         |  status   |         started_at         |        completed_at        | duration_secs | item_count | bytes_mib 
--------------------------------------+---------------+------------------------------+-----------+----------------------------+----------------------------+---------------+------------+-----------
 7166c6a3-28db-41a4-bcc6-698eed69c6ff | ONEDRIVE      | OneDrive                     | COMPLETED | 2026-05-13 23:45:10.972776 | 2026-05-14 01:18:29.230853 |          5598 |        574 |      1483
 8b080334-82b3-47cb-8f2b-d2eaa31fab11 | ONEDRIVE      | OneDrive                     | COMPLETED | 2026-05-13 23:56:38.805226 | 2026-05-14 00:31:28.579877 |          2089 |        265 |       141
 7d99b542-45d0-4af3-865b-731848c736e5 | ONEDRIVE      | OneDrive                     | COMPLETED | 2026-05-13 21:45:41.932576 | 2026-05-13 23:56:31.828107 |          7849 |        384 |       943
 6b2b67e3-5652-49b9-a281-c7efa2cc8672 | ONEDRIVE      | OneDrive                     | COMPLETED | 2026-05-13 21:45:41.932152 | 2026-05-13 23:55:56.342239 |          7814 |        237 |         0
 db12dfd5-ae12-404d-aa53-2352eaf1c044 | ONEDRIVE      | OneDrive                     | COMPLETED | 2026-05-13 23:42:46.676255 | 2026-05-13 23:45:06.017714 |           139 |         22 |        13
 b80f1b2d-2bf6-4127-80b9-2404f8f76732 | ONEDRIVE      | OneDrive                     | COMPLETED | 2026-05-13 23:41:31.347322 | 2026-05-13 23:42:41.803869 |            70 |         13 |         1
 5f0918ed-2169-43a6-a76b-c51792f11495 | ONEDRIVE      | OneDrive                     | COMPLETED | 2026-05-13 23:37:35.137489 | 2026-05-13 23:41:26.404516 |           231 |         34 |        17
 1a406ce6-dfbd-4411-84c4-156c2d46cbf8 | ONEDRIVE      | OneDrive                     | COMPLETED | 2026-05-13 21:45:41.867038 | 2026-05-13 23:37:30.146513 |          6708 |        113 |       100
 09655871-06ea-4d6e-88d7-dcd474f3aed3 | ONEDRIVE      | OneDrive                     | COMPLETED | 2026-05-13 21:45:41.866361 | 2026-05-13 23:35:55.94115  |          6614 |         33 |         0
 d9402fa6-0d72-406b-9742-bb1b36a563c9 | ONEDRIVE      | OneDrive                     | COMPLETED | 2026-05-13 21:45:41.266793 | 2026-05-13 23:27:02.191571 |          6080 |        138 |        78
 42de0797-fefa-4bd3-9463-18ef9532b4a6 | ONEDRIVE      | OneDrive                     | COMPLETED | 2026-05-13 21:45:41.933573 | 2026-05-13 23:15:32.082209 |          5390 |          8 |         0
 a2cd7096-2374-4548-9963-72ffad45c74b | ONEDRIVE      | OneDrive                     | COMPLETED | 2026-05-13 21:45:41.933315 | 2026-05-13 22:55:31.565754 |          4190 |         18 |         0
 ab60d9f6-ad24-4135-a7df-dfd14fdb7e6c | ONEDRIVE      | OneDrive                     | FAILED    | 2026-05-13 21:45:41.932862 | 2026-05-13 22:20:31.566335 |          2090 |          0 |         0
 db784151-f505-4a4b-8869-99c77d33eff5 | ONEDRIVE      | OneDrive                     | FAILED    | 2026-05-13 21:45:41.931416 | 2026-05-13 22:20:31.566335 |          2090 |          0 |         0
 585facc8-163a-429a-a03d-52de083fea93 | MAILBOX       | Vinay Chauhan                | COMPLETED | 2026-05-13 21:45:41.44249  | 2026-05-13 22:12:59.98307  |          1638 |       9102 |       680
 13500240-580a-49ed-bdae-ccb54f062d77 | USER_CHATS    | Chats — Narendra  Chaudhari  | COMPLETED | 2026-05-13 21:47:59.790455 | 2026-05-13 22:12:34.432005 |          1474 |      39574 |      1760
 b961861f-9dbc-4b73-99a3-891a9026751c | USER_CHATS    | Chats — Akshat Verma         | COMPLETED | 2026-05-13 21:47:59.740916 | 2026-05-13 22:12:03.604481 |          1443 |      19697 |       488
 5e5f147f-5c99-4e8f-a9ab-6296c8094c78 | USER_CHATS    | Chats — Amit Mishra          | COMPLETED | 2026-05-13 21:47:59.743298 | 2026-05-13 22:11:11.40411  |          1391 |      29013 |      1960
 94ccfd4c-afab-4db9-b91d-7da418720fda | USER_MAIL     | Mail — Vinay Chauhan         | COMPLETED | 2026-05-13 21:48:00.350703 | 2026-05-13 22:11:08.707789 |          1388 |       7287 |       680
 72032b17-9823-4ecd-bea8-28821e992467 | USER_CHATS    | Chats — Rohit Sharma         | COMPLETED | 2026-05-13 21:47:59.854883 | 2026-05-13 22:10:29.814249 |          1349 |      25869 |      1269
 c17e58da-fe28-49ca-a6d6-50b442c4e27f | USER_CHATS    | Chats — Vinay Chauhan        | COMPLETED | 2026-05-13 21:47:59.88045  | 2026-05-13 22:09:56.346262 |          1316 |      22670 |       923
 e9356892-5cca-46c0-bbf8-61646df23139 | USER_CHATS    | Chats — Ranjeet Singh        | COMPLETED | 2026-05-13 21:47:59.884464 | 2026-05-13 22:09:02.36397  |          1262 |      19787 |       506
 47588efc-bf51-43e8-863b-9d3b30ed5532 | USER_CHATS    | Chats — Gajraj Singh Rathore | COMPLETED | 2026-05-13 21:48:00.228282 | 2026-05-13 22:07:46.26732  |          1186 |       4636 |        63
 3057559b-bcc8-412b-9242-4c2005bee7ee | USER_CHATS    | Chats — Rashmi Mishra        | COMPLETED | 2026-05-13 21:47:59.886657 | 2026-05-13 22:06:39.239464 |          1119 |       5459 |        73
 3b0c4f3f-d8d7-4fe8-94c6-32d5055db351 | USER_CHATS    | Chats — Hemant Singh         | COMPLETED | 2026-05-13 21:48:00.09831  | 2026-05-13 22:04:17.49225  |           977 |       4567 |        79
 a8b3af79-7b61-4385-8eae-fb87ee1bdd2f | USER_MAIL     | Mail — Ranjeet Singh         | COMPLETED | 2026-05-13 21:48:00.593118 | 2026-05-13 22:02:43.97654  |           883 |       2145 |       219
 a2989d03-577c-4079-b197-9bde86047e01 | USER_MAIL     | Mail — Rohit Sharma          | COMPLETED | 2026-05-13 21:48:00.281418 | 2026-05-13 22:02:10.65553  |           850 |       1343 |       116
 f4651066-fdf1-45f0-b1c0-0e2f21badd56 | USER_MAIL     | Mail — Narendra  Chaudhari   | COMPLETED | 2026-05-13 21:48:00.234634 | 2026-05-13 22:01:52.361918 |           832 |       1055 |        95
 2c35b242-74f9-44da-8466-e650ecf007e9 | USER_MAIL     | Mail — Amit Mishra           | COMPLETED | 2026-05-13 21:48:00.231396 | 2026-05-13 22:01:26.272113 |           806 |       1105 |        43
 5bc261ea-26d6-4b36-8b5b-f343bd359996 | USER_MAIL     | Mail — Rashmi Mishra         | COMPLETED | 2026-05-13 21:48:00.378364 | 2026-05-13 22:01:11.127065 |           790 |        392 |         1
(30 rows)

```

## Items by type (counts + bytes)
```
       item_type       |  rows  |          mib           
-----------------------+--------+------------------------
 CHAT_ATTACHMENT       |  20498 |  6931.6885290145874023
 ONEDRIVE_FILE         |   2745 |  4955.0028781890869141
 FILE                  |   2231 |  3974.7728004455566406
 EMAIL_ATTACHMENT      |   6728 |  1872.4996652603149414
 EMAIL                 |  28842 |   483.5704593658447266
 FILE_VERSION          |    443 |   275.3561954498291016
 TEAMS_CHAT_MESSAGE    | 171272 |   194.4997577667236328
 CALENDAR_EVENT        |   3273 |     5.6546325683593750
 USER_GROUP_MEMBERSHIP |     47 | 0.04506206512451171875
 USER_DIRECT_REPORT    |     15 | 0.00546836853027343750
 USER_CONTACT          |      3 | 0.00375747680664062500
 USER_PROFILE          |      9 | 0.00363922119140625000
 USER_MANAGER          |      8 | 0.00362777709960937500
(13 rows)

```

## Throughput per snapshot (top 20 by bytes_added)
```
                  id                  |     type      |          display_name          | item_count | bytes_mib | duration_secs | mib_per_sec 
--------------------------------------+---------------+--------------------------------+------------+-----------+---------------+-------------
 1111ada9-fe42-4a6c-88b6-fe0f9b592618 | USER_ONEDRIVE | OneDrive — Ranjeet Singh       |       1292 |    2108.6 |           536 |        3.93
 5e5f147f-5c99-4e8f-a9ab-6296c8094c78 | USER_CHATS    | Chats — Amit Mishra            |      29013 |    1960.1 |          1391 |        1.41
 13500240-580a-49ed-bdae-ccb54f062d77 | USER_CHATS    | Chats — Narendra  Chaudhari    |      39574 |    1760.4 |          1474 |        1.19
 7166c6a3-28db-41a4-bcc6-698eed69c6ff | ONEDRIVE      | OneDrive                       |        574 |    1483.9 |          5598 |        0.27
 8ef8c3b6-7620-4b00-a519-833cdad402f4 | USER_ONEDRIVE | OneDrive — Amit Mishra         |        548 |    1483.3 |           161 |        9.21
 72032b17-9823-4ecd-bea8-28821e992467 | USER_CHATS    | Chats — Rohit Sharma           |      25869 |    1269.6 |          1349 |        0.94
 13eeacbe-3949-4d87-8f3f-7c615c1bcc43 | USER_ONEDRIVE | OneDrive — Rohit Sharma        |        391 |    1010.9 |           267 |        3.79
 7d99b542-45d0-4af3-865b-731848c736e5 | ONEDRIVE      | OneDrive                       |        384 |     943.7 |          7849 |        0.12
 c17e58da-fe28-49ca-a6d6-50b442c4e27f | USER_CHATS    | Chats — Vinay Chauhan          |      22670 |     923.9 |          1316 |        0.70
 94ccfd4c-afab-4db9-b91d-7da418720fda | USER_MAIL     | Mail — Vinay Chauhan           |       7287 |     680.9 |          1388 |        0.49
 585facc8-163a-429a-a03d-52de083fea93 | MAILBOX       | Vinay Chauhan                  |       9102 |     680.9 |          1638 |        0.42
 e9356892-5cca-46c0-bbf8-61646df23139 | USER_CHATS    | Chats — Ranjeet Singh          |      19787 |     506.6 |          1262 |        0.40
 b961861f-9dbc-4b73-99a3-891a9026751c | USER_CHATS    | Chats — Akshat Verma           |      19697 |     488.9 |          1443 |        0.34
 a8b3af79-7b61-4385-8eae-fb87ee1bdd2f | USER_MAIL     | Mail — Ranjeet Singh           |       2145 |     219.6 |           883 |        0.25
 6e7e371d-0fab-48cb-90cb-5fd4a66d121e | MAILBOX       | Ranjeet Singh                  |       2980 |     219.6 |           887 |        0.25
 de5b8ce7-d7e3-4586-bebd-08ff69aa468c | USER_ONEDRIVE | OneDrive — Vinay Chauhan       |        221 |     141.4 |           195 |        0.73
 8b080334-82b3-47cb-8f2b-d2eaa31fab11 | ONEDRIVE      | OneDrive                       |        265 |     141.1 |          2089 |        0.07
 a2989d03-577c-4079-b197-9bde86047e01 | USER_MAIL     | Mail — Rohit Sharma            |       1343 |     116.5 |           850 |        0.14
 970a61ed-43aa-40bf-8196-78d528186802 | MAILBOX       | Rohit Sharma                   |       1728 |     116.5 |           614 |        0.19
 dc462813-879b-4a2d-b5c1-90052bf7e065 | USER_ONEDRIVE | OneDrive — Narendra  Chaudhari |        114 |     101.7 |            66 |        1.54
(20 rows)

```

## Existing snapshot_partitions (if any — pre-deploy of new partition vars)
```
 partition_type | status | count | files | bytes_mib 
----------------+--------+-------+-------+-----------
(0 rows)

```

## Top users by OneDrive size (the 14 GB backup target & whales)
```
          display_name          |     type      | storage_gib |       last_backup_at       
--------------------------------+---------------+-------------+----------------------------
 OneDrive — Ranjeet Singh       | USER_ONEDRIVE |        2.06 | 2026-05-13 21:56:54.808592
 OneDrive                       | ONEDRIVE      |        1.45 | 2026-05-14 01:18:33.991752
 OneDrive — Amit Mishra         | USER_ONEDRIVE |        1.45 | 2026-05-13 21:50:38.861715
 OneDrive — Rohit Sharma        | USER_ONEDRIVE |        0.99 | 2026-05-13 21:52:25.330863
 OneDrive                       | ONEDRIVE      |        0.92 | 2026-05-13 23:56:36.681025
 OneDrive — Vinay Chauhan       | USER_ONEDRIVE |        0.14 | 2026-05-13 21:51:13.740502
 OneDrive                       | ONEDRIVE      |        0.14 | 2026-05-14 00:31:33.532912
 OneDrive — Narendra  Chaudhari | USER_ONEDRIVE |        0.10 | 2026-05-13 21:49:04.897299
 OneDrive                       | ONEDRIVE      |        0.10 | 2026-05-13 23:37:33.55068
 OneDrive — Akshat Verma        | USER_ONEDRIVE |        0.08 | 2026-05-13 21:48:35.216089
 OneDrive                       | ONEDRIVE      |        0.08 | 2026-05-13 23:27:40.503262
 OneDrive — Rashmi Mishra       | USER_ONEDRIVE |        0.02 | 2026-05-13 21:52:35.963519
 OneDrive                       | ONEDRIVE      |        0.02 | 2026-05-13 23:41:29.810554
 OneDrive — Hemant Singh        | USER_ONEDRIVE |        0.01 | 2026-05-13 21:48:44.807107
 OneDrive                       | ONEDRIVE      |        0.01 | 2026-05-13 23:45:09.421659
(15 rows)

```

## Mail backup sizes (USER_MAIL / MAILBOX / SHARED_MAILBOX / ROOM_MAILBOX)
```
        display_name        |   type    | storage_mib |       last_backup_at       
----------------------------+-----------+-------------+----------------------------
 Mail — Vinay Chauhan       | USER_MAIL |       680.9 | 2026-05-13 22:11:09.948088
 Vinay Chauhan              | MAILBOX   |       680.9 | 2026-05-13 22:13:01.265056
 Mail — Ranjeet Singh       | USER_MAIL |       219.6 | 2026-05-13 22:02:44.599115
 Ranjeet Singh              | MAILBOX   |       219.6 | 2026-05-13 22:00:31.05424
 Mail — Rohit Sharma        | USER_MAIL |       116.5 | 2026-05-13 22:02:11.279686
 Rohit Sharma               | MAILBOX   |       116.5 | 2026-05-13 21:55:57.720519
 Narendra  Chaudhari        | MAILBOX   |        95.3 | 2026-05-13 21:53:23.073892
 Mail — Narendra  Chaudhari | USER_MAIL |        95.3 | 2026-05-13 22:01:53.008872
 Mail — Amit Mishra         | USER_MAIL |        43.7 | 2026-05-13 22:01:28.356787
 Amit Mishra                | MAILBOX   |        43.7 | 2026-05-13 21:57:46.122213
 Akshat Verma               | MAILBOX   |        16.1 | 2026-05-13 21:52:28.023004
 Mail — Akshat Verma        | USER_MAIL |        16.1 | 2026-05-13 22:01:08.833295
 Mail — Hemant Singh        | USER_MAIL |         2.1 | 2026-05-13 22:01:10.188542
 Hemant Singh               | MAILBOX   |         2.1 | 2026-05-13 21:48:16.404821
 Rashmi Mishra              | MAILBOX   |         2.0 | 2026-05-13 21:50:49.386824
(15 rows)

```

## Chats backup sizes (USER_CHATS)
```
         display_name         |    type    | storage_mib |       last_backup_at       
------------------------------+------------+-------------+----------------------------
 Chats — Amit Mishra          | USER_CHATS |      1960.1 | 2026-05-13 22:11:11.957982
 Chats — Narendra  Chaudhari  | USER_CHATS |      1760.4 | 2026-05-13 22:12:35.043969
 Chats — Rohit Sharma         | USER_CHATS |      1269.6 | 2026-05-13 22:10:30.435291
 Chats — Vinay Chauhan        | USER_CHATS |       923.9 | 2026-05-13 22:09:56.95845
 Chats — Ranjeet Singh        | USER_CHATS |       506.6 | 2026-05-13 22:09:02.98339
 Chats — Akshat Verma         | USER_CHATS |       488.9 | 2026-05-13 22:12:04.220182
 Chats — Hemant Singh         | USER_CHATS |        79.7 | 2026-05-13 22:04:18.841914
 Chats — Rashmi Mishra        | USER_CHATS |        73.0 | 2026-05-13 22:06:39.861095
 Chats — Gajraj Singh Rathore | USER_CHATS |        64.0 | 2026-05-13 22:07:46.884026
(9 rows)

```

## Cluster-wide totals (the 'how much data is in this Railway DB right now' number)
```
 total_snapshots | total_items | total_gib_backed_up | avg_snapshot_duration_secs | max_snapshot_duration_secs 
-----------------+-------------+---------------------+----------------------------+----------------------------
              75 |      211417 |               16.82 |                     1052.0 |                     7849.0
(1 row)

```

## Bulk-backup job duration (Activity-row equivalent — wall time of bulk parent jobs)
```
ERROR:  column "started_at" does not exist
LINE 1: ...ch_path=tm_vault,public; SELECT id, type, status, started_at...
                                                             ^
```

## SeaweedFS volume size estimate (computed from snapshot_items.content_size sum)
```
 total_blob_mib | blob_rows | inline_rows 
----------------+-----------+-------------
        18693.3 |      9654 |      226464
(1 row)

```

## Per-resource per-type backup duration histogram (the comparison target)

For each (resource_type, snapshot status), shows count / avg wall / p95 wall / total bytes.

```
     type      |  status   | n  | avg_secs | p50_secs | p95_secs | max_secs | total_mib 
---------------+-----------+----+----------+----------+----------+----------+-----------
 MAILBOX       | COMPLETED |  9 |    578.7 |    460.0 |   1337.6 |     1638 |    1177.1
 ONEDRIVE      | COMPLETED | 12 |   4397.7 |   5494.0 |   7829.8 |     7849 |    2781.1
 ONEDRIVE      | FAILED    |  2 |   2090.0 |   2090.0 |   2090.0 |     2090 |       0.0
 ENTRA_USER    | COMPLETED |  9 |     21.6 |     22.0 |     24.0 |       24 |       0.1
 USER_MAIL     | COMPLETED |  9 |    812.9 |    806.0 |   1186.0 |     1388 |    1177.1
 USER_ONEDRIVE | COMPLETED |  9 |    177.7 |    161.0 |    432.4 |      536 |    4958.8
 USER_CONTACTS | COMPLETED |  9 |     12.8 |     12.0 |     16.8 |       18 |       0.0
 USER_CALENDAR | COMPLETED |  9 |     20.2 |     17.0 |     35.2 |       44 |       5.7
 USER_CHATS    | COMPLETED |  9 |   1279.7 |   1316.0 |   1461.6 |     1474 |    7126.2
(9 rows)

```

## Largest single-resource backup durations (the slow ones — partition target)
```
    type    |        display_name         | item_count |  mib   | duration_secs | minutes |        completed_at        
------------+-----------------------------+------------+--------+---------------+---------+----------------------------
 ONEDRIVE   | OneDrive                    |        384 |  943.7 |          7849 |   130.8 | 2026-05-13 23:56:31.828107
 ONEDRIVE   | OneDrive                    |        237 |    0.0 |          7814 |   130.2 | 2026-05-13 23:55:56.342239
 ONEDRIVE   | OneDrive                    |        113 |  100.8 |          6708 |   111.8 | 2026-05-13 23:37:30.146513
 ONEDRIVE   | OneDrive                    |         33 |    0.0 |          6614 |   110.2 | 2026-05-13 23:35:55.94115
 ONEDRIVE   | OneDrive                    |        138 |   78.5 |          6080 |   101.3 | 2026-05-13 23:27:02.191571
 ONEDRIVE   | OneDrive                    |        574 | 1483.9 |          5598 |    93.3 | 2026-05-14 01:18:29.230853
 ONEDRIVE   | OneDrive                    |          8 |    0.0 |          5390 |    89.8 | 2026-05-13 23:15:32.082209
 ONEDRIVE   | OneDrive                    |         18 |    0.0 |          4190 |    69.8 | 2026-05-13 22:55:31.565754
 ONEDRIVE   | OneDrive                    |        265 |  141.1 |          2089 |    34.8 | 2026-05-14 00:31:28.579877
 MAILBOX    | Vinay Chauhan               |       9102 |  680.9 |          1638 |    27.3 | 2026-05-13 22:12:59.98307
 USER_CHATS | Chats — Narendra  Chaudhari |      39574 | 1760.4 |          1474 |    24.6 | 2026-05-13 22:12:34.432005
 USER_CHATS | Chats — Akshat Verma        |      19697 |  488.9 |          1443 |    24.1 | 2026-05-13 22:12:03.604481
 USER_CHATS | Chats — Amit Mishra         |      29013 | 1960.1 |          1391 |    23.2 | 2026-05-13 22:11:11.40411
 USER_MAIL  | Mail — Vinay Chauhan        |       7287 |  680.9 |          1388 |    23.1 | 2026-05-13 22:11:08.707789
 USER_CHATS | Chats — Rohit Sharma        |      25869 | 1269.6 |          1349 |    22.5 | 2026-05-13 22:10:29.814249
(15 rows)

```

## Backup jobs (wall-time = completed_at - created_at)
```
                  id                  |  type  |  status   |         created_at         |        completed_at        | wall_secs | wall_min | bytes_mib | items_processed | resource_count 
--------------------------------------+--------+-----------+----------------------------+----------------------------+-----------+----------+-----------+-----------------+----------------
 c92862ad-5855-4c02-8cbc-6f4a3f2dd4d6 | BACKUP | COMPLETED | 2026-05-13 21:47:52.388773 | 2026-05-13 21:57:01.224477 |       549 |      9.1 |       0.0 |               0 |              9
 1739095a-c153-4c61-a871-0785c50b702f | BACKUP | COMPLETED | 2026-05-13 21:47:52.388763 | 2026-05-13 22:12:39.756925 |      1487 |     24.8 |       0.0 |               0 |             36
 4e2444ff-2313-4088-8dd0-32372d30a620 | BACKUP | COMPLETED | 2026-05-13 21:45:35.851312 | 2026-05-13 22:13:03.71597  |      1648 |     27.5 |       0.0 |               0 |             18
 c5e8de87-cc82-4e9c-8df2-2af51d3b4b32 | BACKUP | COMPLETED | 2026-05-13 21:45:35.8513   | 2026-05-13 23:42:44.630927 |      7029 |    117.1 |     198.8 |             357 |              9
(4 rows)
```

**Key takeaway**: the longest bulk-backup job ran **117 min for 9 resources (357 items, 198.8 MB processed)** — that's the wall-time the partition fanout will compress. With 4-shard partitioning + 4 worker replicas, the same 9-resource job should drop to **~30-40 min** wall-time.

## SeaweedFS volume disk usage (actual blob storage on Railway disk)
```
Filesystem                Size      Used Available Use% Mounted on
/dev/zd17280             45.5G     17.1G     28.4G  38% /data

Total /data usage: 17.1 GiB on 45.5 GiB volume (38% full)
```

---

## Key benchmark headline numbers (compare against after-partition run)

| Metric | Pre-partition baseline |
|---|---|
| Total snapshots (all-time) | 78 |
| Total items across all backups | 236,114 |
| Total tracked bytes (DB sum) | check throughput-per-snapshot table |
| SeaweedFS actual disk usage | 17.1 GiB |
| Tenants | 1 |
| Resources tracked | 141 |
| Snapshot partitions used | 0 (clean baseline — partition path never triggered) |

### Worst slow backups (the ones partitioning will fix):
- ONEDRIVE 7849s (~131 min) for 943 MiB
- ONEDRIVE 7814s (~130 min) for 0 MiB (failed/empty)
- ONEDRIVE 6708s (~112 min)
- ONEDRIVE 6614s (~110 min)
- ONEDRIVE 6080s (~101 min)
- ONEDRIVE 5598s (~93 min) for 1.48 GB ← the **closest comparison** to the 14 GB you mentioned
- ONEDRIVE 5390s (~90 min)
- MAILBOX 1638s (~27 min) for 680 MiB (Vinay's mailbox)
- USER_CHATS 1474s (~25 min) for 1.76 GiB (Narendra's chats)

### After-partition expectation:
- 1-2 GiB OneDrive: 22 min → ~5-7 min (3-4× speedup)
- 600 MiB mailbox: 27 min → ~7-10 min
- 1.7 GiB chats: 25 min → ~8-12 min
- Incremental (no changes): seconds (delta tokens populate post-baseline)

