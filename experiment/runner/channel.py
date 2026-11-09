"""Encrypted channel between the lab VMs, for the D4 overhead test .

The migration (and every other VM-to-VM flow of a run) normally goes over the lab network in
plaintext, like the real-WAN ladder did. To measure what encrypting it costs:

  wireguard   wg0 in both VMs (10.77.0.10 <-> 10.77.0.20, MTU 1308 = lab 1388 - WireGuard's 80 B),
              fresh keys per `up`, kept in /tmp only. A profile whose destination.public_ip is
              10.77.0.20 (hosts.lab.wg.server.yaml) sends everything through it. The emulator
              still matches the OUTER packets (lab IPs), so the encrypted traffic crosses the
              emulated WAN like the real internet.
  ssh         the runner's SSH tunnel (source.tunnel in the profile: hosts.lab.ssh.server.yaml):
              ssh -L for the migration port only, from the source VM to the destination VM.
              A throwaway key pair is created for it and its public half authorized on the
              destination VM until `down`.

`down` removes everything `up` created; nothing survives a VM re-clone anyway.

    python3 channel.py --config hosts.lab.server.yaml up wireguard
    python3 channel.py --config hosts.lab.server.yaml down
"""
import argparse
import sys

import run_experiment as rx

WG_NET = {"src": "10.77.0.10", "dst": "10.77.0.20"}
WG_MTU = 1308
WG_PORT = 51820
TUN_KEY = "/tmp/statepod_tunnel_key"
TUN_MARK = "statepod-lab-tunnel"


def wg_up(cfg):
    src, dst = cfg["source"]["ssh"], cfg["destination"]["ssh"]
    lab = {"src": cfg["netem"]["src_ip"], "dst": cfg["netem"]["dst_ip"]}
    for alias in (src, dst):
        rx.ssh(alias, "command -v wg >/dev/null || sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -q "
                      "wireguard-tools >/dev/null", timeout=300)
    pub = {}
    for role, alias in (("src", src), ("dst", dst)):
        pub[role] = rx.ssh(alias, "umask 077; wg genkey > /tmp/statepod_wg.key; wg pubkey < /tmp/statepod_wg.key",
                           timeout=30).stdout.strip()
    for role, alias in (("src", src), ("dst", dst)):
        peer = "dst" if role == "src" else "src"
        rx.ssh(alias, f"sudo ip link del wg0 2>/dev/null; sudo ip link add wg0 type wireguard && "
                      f"sudo wg set wg0 listen-port {WG_PORT} private-key /tmp/statepod_wg.key "
                      f"peer {pub[peer]} endpoint {lab[peer]}:{WG_PORT} allowed-ips {WG_NET[peer]}/32 "
                      f"persistent-keepalive 25 && "
                      f"sudo ip addr add {WG_NET[role]}/24 dev wg0 && sudo ip link set wg0 mtu {WG_MTU} up",
               timeout=30)
    out = rx.ssh(src, f"ping -c 3 -W 2 {WG_NET['dst']} | tail -1", check=False).stdout.strip()
    print(f"wireguard up: {WG_NET['src']} <-> {WG_NET['dst']}, mtu {WG_MTU}; ping {out}")


def wg_down(cfg):
    for alias in (cfg["source"]["ssh"], cfg["destination"]["ssh"]):
        rx.ssh(alias, "sudo ip link del wg0 2>/dev/null; rm -f /tmp/statepod_wg.key; true", check=False)


def ssh_up(cfg):
    src, dst = cfg["source"]["ssh"], cfg["destination"]["ssh"]
    pub = rx.ssh(src, f"rm -f {TUN_KEY} {TUN_KEY}.pub; ssh-keygen -q -t ed25519 -N '' -C {TUN_MARK} "
                      f"-f {TUN_KEY} </dev/null && cat {TUN_KEY}.pub", timeout=30).stdout.strip()
    rx.ssh(dst, f"grep -v {TUN_MARK} ~/.ssh/authorized_keys > /tmp/ak.$$; cat /tmp/ak.$$ > ~/.ssh/authorized_keys; "
                f"rm -f /tmp/ak.$$; echo '{pub}' >> ~/.ssh/authorized_keys", timeout=30)
    print(f"ssh tunnel key ready: {TUN_KEY} on the source VM, authorized on the destination VM "
          f"(the runner opens the tunnel per run, source.tunnel in the profile)")


def ssh_down(cfg):
    rx.ssh(cfg["destination"]["ssh"], f"grep -v {TUN_MARK} ~/.ssh/authorized_keys > /tmp/ak.$$; "
                                       f"cat /tmp/ak.$$ > ~/.ssh/authorized_keys; rm -f /tmp/ak.$$", check=False)
    rx.ssh(cfg["source"]["ssh"], f"pkill -f '[s]sh .*-L .*8080' ; rm -f {TUN_KEY} {TUN_KEY}.pub; true", check=False)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="hosts.lab.server.yaml")
    ap.add_argument("action", choices=["up", "down"])
    ap.add_argument("kind", nargs="?", choices=["wireguard", "ssh"])
    a = ap.parse_args()
    cfg = rx.load_config(a.config)
    if a.action == "down":
        wg_down(cfg)
        ssh_down(cfg)
        print("channels down")
        return 0
    if not a.kind:
        sys.exit("up needs a kind: wireguard | ssh")
    (wg_up if a.kind == "wireguard" else ssh_up)(cfg)
    return 0


if __name__ == "__main__":
    sys.exit(main())
