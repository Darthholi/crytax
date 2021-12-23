import ccxt

exchangeCoinbaseProSandbox = ccxt.coinbasepro({
    "apiKey": "apk",
    "secret": "sec",
    "password": "psw",
    "enableRateLimit": True,
    "sandbox": True
})

exchanges = [exchangeCoinbaseProSandbox]
