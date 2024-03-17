# CryTax

Crypto taxing helper using ccxt.
First run cry_download and then tax_sum_cz.

Currently ccxt history cannot get the following:
- bitstamp ()
- coinbase (not pro - https://blog.coinbase.com/you-can-now-export-your-transaction-history-c21ca0a50bca, https://www.coinbase.com/reports)
- crypto.com (https://help.crypto.com/en/articles/3438579-how-do-i-export-my-transaction-history-app)
... and you need to follow their own guide for exporting to pdf/csv.

Warning:
- Does not work for Huobi, because of their poor implementation/procedures:
  - https://www.reddit.com/r/huobi/comments/b5t8g3/trade_history_for_taxes/
  - https://github.com/ccxt/ccxt/issues/6512
  - (Fortunately huobi only trades crypto, not fiats if it helps for the taxes...)
