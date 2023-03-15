# openbook-candles

OpenBook Candles is an open source trade scraper and candle batcher combined with web API for OpenBook frontends. The web API is largely based off of the code [here](https://github.com/Bonfida/agnostic-candles).


[Configuration](#configuration)  
[Worker](#worker)  
[Server](#server)

<a name="configuration"></a>
<h2 align="center">Configuration</h2>
<br />

>⚠️    This repo requires that Postgres be used as the database to store trades and candles.

See the .env file for providing Postgres configuration options and a Solana RPC URL. Markets should be held in a JSON file in the repo's root as follows:


```json
[
  {
    "name" : "SOL/USDC",
    "address" : "8BnEgHoWFysVcuFFX7QztDmzuH8r5ZFvyP3sYwn1XTh6"
  },
  {
    "name" : "BONK/SOL",
    "address" : "Hs97TCZeuYiJxooo3U73qEHXg3dKpRL4uYKYRryEK9CF"
  }
]
```

<br />
<a name="worker"></a>
<h2 align="center">Worker</h2>
<br />

The worker directory contains the program that scrapes OpenBook trades and stores them. The worker is also responsible for batching the trades into OHLCV candles.

To run the worker locally:

```
cargo run --bin worker markets_json_path
```

- `markets_json_path` is the path to your JSON file that contains the markets you want to fetch


<br />

The worker uses [getConfirmedSignaturesForAddress2](https://docs.solana.com/api/http#getconfirmedsignaturesforaddress2) to scrape OpenBook trades. Only trades from the specified markets will be saved. Each market will automatically batch 1,3,5,15,30 minute, 1,2,4 hour, and 1 day candles from the scraped trades.


<br />
<a name="server"></a>
<h2 align="center">Server</h2>
<br />

The server uses [actix web](https://actix.rs/) and is served by default on port `8080` .

To run the server locally:

```
cargo run markets_json_path 
```
- `markets_json_path` is the path to your JSON file that contains the markets you want to fetch

The server supports the following endpoints:


### Markets

**Request:**

`GET api/markets`

Show all markets available via the API

**Response:**

```json
[
  {
    "name" : "SOL/USDC",
    "address" : "8BnEgHoWFysVcuFFX7QztDmzuH8r5ZFvyP3sYwn1XTh6"
  },
  {
    "name" : "BONK/SOL",
    "address" : "Hs97TCZeuYiJxooo3U73qEHXg3dKpRL4uYKYRryEK9CF"
  }
]
```

### Candles

**Request:**

`GET /api/candles?market_name={market_name}&from={from}&to={to}&resolution={resolution}`


Returns historical candles

**Response:**

```json
{
  "s": "ok",
  "time": [1651189320, 1651189380],
  "close": [1.2090027797967196, 1.2083083698526025],
  "open": [1.2090027797967196, 1.208549999864772],
  "high": [1.2090027797967196, 1.208549999864772],
  "low": [1.2090027797967196, 1.208055029856041],
  "volume": [0, 0]
}
```


Note that if `market_name` contains a forward slash, it will need to be delimited.  
For example: `GET /api/candles?market_name=SOL%2FUSDC&from=1678425243&to=1678725243&resolution=1M`

### Traders (By Base Token Volume)

**Request:**

`GET /api/traders/base-volume?market_name={market_name}&from={from}&to={to}`


Returns the top traders sorted by base token volume (limited to 10,000)

**Response:**

```json
{
  "start_time": 1678425243,
  "end_time": 1678725243,
  "volume_type": "Base",
  "traders": [
        {
          "pubkey": "JCNCMFXo5M5qwUPg2Utu1u6YWp3MbygxqBsBeXXJfrw",
          "volume": 32372.207
        },
        {
          "pubkey": "dSaHguZBem6EhwBtyDECVmCwsWirH1Dh2i2PpG8e7mF",
          "volume": 29923.218
        },
        {
          "pubkey": "ASx1wk74GLZsxVrYiBkNKiViPLjnJQVGxKrudRgPir4A",
          "volume": 24492.741
        },
        {
          "pubkey": "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1",
          "volume": 8762.088
        },
        {
          "pubkey": "G6EF4Q1mJBZck6BNrMk6xMkCC8Su7GrS1U1HxoRW5hAV",
          "volume": 8615.63
        },
        {
          "pubkey": "B7eMbqxyR57WsKp3Nr3dacVYTJdEnJsQF78C4mzw2wCm",
          "volume": 5434.494
        },
        {
          "pubkey": "GroundbRKWG9T6b3BTc1GN7QKRavM6VspX7mkheL6oq2",
          "volume": 2994.169
        }
    ]
}

```

### Traders (By Quote Token Volume)

**Request:**

`GET /api/traders/quote-volume?market_name={market_name}&from={from}&to={to}`


Returns the top traders sorted by quote token volume (limited to 10,000)

**Response:**

```json
{
  "start_time": 1678425243,
  "end_time": 1678725243,
  "volume_type": "Quote",
  "traders": [
        {
          "pubkey": "JCNCMFXo5M5qwUPg2Utu1u6YWp3MbygxqBsBeXXJfrw",
          "volume": 643653.147668
        },
        {
          "pubkey": "dSaHguZBem6EhwBtyDECVmCwsWirH1Dh2i2PpG8e7mF",
          "volume": 595895.49311
        },
        {
          "pubkey": "ASx1wk74GLZsxVrYiBkNKiViPLjnJQVGxKrudRgPir4A",
          "volume": 508368.348359
        },
        {
          "pubkey": "G6EF4Q1mJBZck6BNrMk6xMkCC8Su7GrS1U1HxoRW5hAV",
          "volume": 174858.502941
        },
        {
          "pubkey": "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1",
          "volume": 174065.365701
        },
        {
          "pubkey": "B7eMbqxyR57WsKp3Nr3dacVYTJdEnJsQF78C4mzw2wCm",
          "volume": 107567.684933
        },
        {
          "pubkey": "GroundbRKWG9T6b3BTc1GN7QKRavM6VspX7mkheL6oq2",
          "volume": 59100.773085
        }
    ]
}
