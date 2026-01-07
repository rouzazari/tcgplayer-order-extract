# tcgplayer-order-extract
Extract Order Information JSONs from tcgplayer orders

Currently, works best with logging in manually and saving cookies.

## Examples of usage:

### Saves to S3 bucket for 12/22/2025 to 12/31/2025 using cookies for login. Only checks "Normal" orders (skips Direct). Skips existing orders.
```commandline
python -m tcgplayer_order_extract.main --storage-type "S3Storage" --bucket-name "tcgplayer-orders" --date-from "12/22/2025" --date-to "12/31/2025" --login "cookies-only" --order-type "Normal" --skip-existing
```

### Save order JSONs to S3 bucket for 12/01/2025 to 01/05/2026 using cookies for login. Only checks Direct orders (skips "Normal"). Checks md5 of downloaded files versus S3 and only uploads if different.
```commandline
python -m tcgplayer_order_extract.main --storage-type "S3Storage" --bucket-name "tcgplayer-orders" --date-from "12/01/2025" --date-to "01/05/2026" --login "cookies-only" --check-md5 --order-type "Direct"
```
### Copy all files from S3 bucket to local directory.
```commandline
python -m tcgplayer_order_extract.storage --bucket-name "tcgplayer-orders" --base-path "C:\temp\orders" --action "copy-s3-to-local"
```

### Generate reports for all orders in local folder
```commandline
python -m tcgplayer_order_extract.process
```

