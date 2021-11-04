#!/bin/bash
#根据IP进行限速


#清空原有规则（假设网络接口为eth0）
tc qdisc del dev eth0 root
#创建根qdisc
tc qdisc add dev eth0 root handle 1: htb default 1
#tc  qdisc  add  dev eth0  root  handle  10: cbq  bandwidth  100Mbit avpkt  1000
#创建一个主分类绑定所有带宽资源（20M），在没有获得token时，最多允许15KB数据离队
tc class add dev eth0 parent 1:0 classid 1:1 htb rate 100Mbit burst 15k

#创建子分类
tc class add dev eth0 parent 1:1 classid 1:10 htb rate 80Mbit ceil 80Mbit burst 15k  #针对rack外部
tc class add dev eth0 parent 1:1 classid 1:20 htb rate 100Mbit ceil 100Mbit burst 15k  #针对rack内部
  
#避免一个ip霸占带宽资源
tc qdisc add dev eth0 parent 1:10 handle 10: sfq perturb 10
tc qdisc add dev eth0 parent 1:20 handle 20: sfq perturb 10

#创建过滤器
#rack外部限速（80M）
INET=192.168.1.          #(设置网段，根据你的情况填）
IPS=1                         # (这个意思是从192.168.0.1开始）
IPE=2
COUNTER=$IPS
while  [  $COUNTER  -le  $IPE  ]
		do
		tc filter add dev eth0 protocol ip parent 1:0 prio 2 u32 match ip dst $INET$COUNTER flowid 1:10
done

#对rack内部限速（100M）
IPS=3                         # (这个意思是从192.168.0.1开始）
IPE=8
COUNTER=$IPS
while  [  $COUNTER  -le  $IPE  ]
    do
		#对rack内部限速（100M）
		tc filter add dev eth0 protocol ip parent 1:0 prio 1 u32 match ip dst $INET$COUNTER flowid 1:20
done