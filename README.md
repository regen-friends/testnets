# Regen Ledger Testnets

Testnets for [Regen Ledger](https://github.com/regen-network/regen-ledger)

## Join `regen-test-1001` Public Testnet

`regen-test-1001` is now live!

We have a working fork of [Lunie](https://github.com/luniehq/lunie) at https://lunie.regen.network
and a fork of [Big Dipper](https://github.com/forbole/big_dipper) at at https://bigdipper.regen.network/

The genesis files are in [./latest](latest) and the incentive point system in [./regen-test-1001/README.md](./regen-test-1001/README.md)

We have airdropped testnet tokens to all participants in the last Cosmos Hub
testnet. If you would like to participate and have not received tokens, you
can get some from this faucet: http://regen.coris.network/faucet,
ask in our validator telegram channel: https://t.me/joinchat/FJGNSxOpjJcgrUGwAAOKUg
or open an issue with an address and we'll send you some.

For those wanting to develop against the Regen test network APIs, please use the following highly available service provided by [Chorus One](https://chorus.one):
* **RPC**: https://regen.chorus.one:26657
* **LCD**: https://regen-lcd.chorus.one:1317

## How to Run a Testnet Validator

Please refer to the Cosmos Hub documentation on validators for a general overview of running a validator. We are using the exact same validator model and software, but with slightly different parameters and other functionality specific to Regen Network.

* [Run a Validator](https://cosmos.network/docs/cosmos-hub/validators/validator-setup.html)
* [Validators Overview](https://cosmos.network/docs/cosmos-hub/validators/overview.html)
* [Validator Security](https://cosmos.network/docs/cosmos-hub/validators/security.html)
* [Validator FAQ](https://cosmos.network/docs/cosmos-hub/validators/validator-faq.html)

### Prerequisites
```
$ sudo apt-get install gcc g++
```
### Install GO
```
$ wget https://raw.githubusercontent.com/jim380/node_tooling/master/Cosmos/CLI/go_install.sh
$ chmod +x go_install.sh
$ ./go_install.sh -v 1.12.5
```
At the time of this writing, `1.12.5` is the latest version of Golang. **Go 1.12+ is required for the Cosmos SDK.**
### Install XRN
```
$ mkdir -p $GOPATH/src/github.com/regen
$ cd $GOPATH/src/github.com/regen
$ git clone -b <latest-release-tag> https://github.com/regen-network/regen-ledger
$ cd regen-ledger
$ make install
```
Find the latest release tags [here](https://github.com/regen-network/regen-ledger/releases). To verify if installation was successful:
```
$ xrnd version --long
$ xrncli version --long
```
### Setting Up a New Node
```
$ xrnd init --chain-id=regen-test-1001 <your_moniker>
$ xrncli keys add <your_wallet_name>

##
```
**Make sure you back up the mnemonics !!!**

### Creating a Validator
*If you are joining at genesis scroll down to the section on Creating a Genesis Validator!*

Please follow the documentation provided on [creating a validator for Cosmos hub](https://github.com/cosmos/gaia/blob/master/docs/validators/validator-setup.md#create-your-validator), replacing `gaiad` and `gaiacli` with `xrnd` and `xrncli` respectively. Also our testnet staking token denomination is `tree` and Regen addresses begin with `xrn:` instead of `cosmos`.

### Genesis & Seeds
Fetch `genesis.json` into `xrnd`'s `config` directory.
```
$ curl https://raw.githubusercontent.com/regen-network/testnets/master/regen-test-1001/genesis.json > $HOME/.xrnd/config/genesis.json
```
Add seed nodes in `config.toml`.
```
$ nano $HOME/.xrnd/config/config.toml
```
Find the following section and add the seed nodes.
```
# Comma separated list of seed nodes to connect to
seeds = "15ee12ae5fe8256ee94d1065e0000893e52532d9@regen-seed-eu.chorus.one:36656,ca130fd7ca16a957850a96ee9bdb74a351c4929f@regen-seed-us.chorus.one:36656"
```
### Make `xrnd` a System Service (optional)
```
$ sudo nano /lib/systemd/system/xrnd.service
```
Paste in the following:
```
[Unit]
Description=Regen Xrnd
After=network-online.target

[Service]
User=<your_user>
ExecStart=/home/<your_user>/go_workspace/bin/xrnd start
StandardOutput=file:/var/log/xrnd/xrnd.log
StandardError=file:/var/log/xrnd/xrnd_error.log
Restart=always
RestartSec=3
LimitNOFILE=4096

[Install]
WantedBy=multi-user.target
```
**This tutorial assumes `$HOME/go_workspace` to be your Go workspace. Your actual workspace directory may vary.**
#### Start Node
**Method 1** - With `systemd`
```
$ sudo systemctl enable xrnd
$ sudo systemctl start xrnd
```
Check node status
```
$ xrncli status
```
Check logs
```
$ sudo journalctl -u xrnd -f
```
**Method 2** - Without `systemd`
```
$ xrnd start
```
Check node status
```
$ xrncli status
```

### Creating a Genesis Validator

*This section applies ONLY if you are joining at genesis! Genesis for Regen Test-1001 was in June 2019.*
#### Generate Genesis Transaction (optional)
```
$ xrnd add-genesis-account $(xrncli keys show <your_wallet_name> -a) 1000000tree,1000000validatortoken
$ xrnd gentx --name <your_wallet_name> --amount 1000000tree
```
If all goes well, you will see the following message:
```
Genesis transaction written to "/home/user/.xrnd/config/gentx/gentx-f8038a89034kl987ebd493b85a125624d5f4770.json"
```
#### Submit Gentx (optional)
Submit your gentx in a PR [here](https://github.com/regen-network/testnets) 


# Historic Testnets (not in use)

The testnets listed below are no longer active but are retained here for posterity. Do not waste your time trying to join them :)

## `regen-test-1000` 

`regen-test-1000` hit some weird consensus error on app state at block 2.

### `xrn-test-3`

Testnet `xrn-test-3` started producing blocks at `2019-03-29T19:44:44.571815638Z` and is now defunct.


`xrncli` can be configured to connect to the testnet as follows:

```sh
xrncli init --chain-id xrn-test-2 --node tcp://xrn-us-east-1.regen.network:26657
```

### `xrn-test-2`

Deployed at `2018-12-19T20:40:06.463846Z`.

### `xrn-1`

The initial Regen Ledger testnet `xrn-1` was deployed on 2018-12-19.

```
