#!/bin/bash

echo "223B VM installation script -- Written by Stew, modified by Zac"
echo "Default Install includes vim, rust toolchain"

echo "moving to home directory"
pushd "$HOME"  || exit
pwd

echo "installing Vim"
sudo apt install -y vim

echo "installing curl"
sudo apt install -y curl

echo "installing rustc and cargo with rustup"
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

source "$HOME/.cargo/env"
