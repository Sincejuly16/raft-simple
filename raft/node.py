# coding: utf-8

"""
Chris Zhou
2020-2-24
"""

import os
import json
import time
import socket
import random
import logging

from raft.log import Log

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


class Node(object):
    def __init__(self, conf):
        self.role = "follower"
        self.id = conf["id"]
        self.addr = conf["addr"]
        self.peers = conf["peers"]

        # 持久化保存的状态
        self.current_term = 0
        self.voted_for = None

        # 建立节点日志文件夹
        if not os.path.exists(self.id):
            os.mkdir(self.id)

        # 初始化并读取日志
        self.load()
        self.log = Log(self.id)

        # 非持久保存的状态
        # 已经提交的日志索引
        self.commit_index = 0
        # 已经执行的日志索引
        self.last_applied = 0

        # Leader初始化
        # 下一条要复制的日志
        self.next_index = {_id: self.log.last_log_index + 1 for _id in self.peers}
        # follower与leader最后一条匹配的日志
        self.match_index = {_id: -1 for _id in self.peers}

        # 添加日志条目
        self.leader_id = None

        # 请求投票
        self.vote_ids = {_id: 0 for _id in self.peers}

        self.client_addr = None

        # 时钟
        self.wait_ms = (10, 20)
        self.next_leader_election_time = time.time() + random.randint(*self.wait_ms)
        self.next_heartbeat_time = 0

        # 消息
        self.ss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.ss.bind(self.addr)
        self.ss.settimeout(2)

        self.cs = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def load(self):
        file_path = self.id + "/key.json"
        if os.path.exists(file_path):
            with open(file_path, "r") as f:
                data = json.load(f)

            self.current_term = data["current_term"]
            self.voted_for = data["voted_for"]
        else:
            self.save()

    def save(self):
        data = {
            "current_term": self.current_term,
            "voted_for": self.voted_for,
        }

        file_path = self.id + "/key.json"
        logging.info("debug: " + str(data["current_term"]) + " " + str(data["voted_for"]))
        with open(file_path, "w") as f:
            json.dump(data, f)

    def send(self, msg, addr):
        msg = json.dumps(msg).encode("utf-8")
        self.cs.sendto(msg, addr)

    def recv(self):
        msg, addr = self.ss.recvfrom(65535)
        return json.loads(msg), addr

    def redirect(self, data, addr):
        if data is None:
            return None

        if data["type"] == "client_append_entries":
            # 若当前节点不是leader, 将请求重定向到leader节点
            if self.role != "leader":
                if self.leader_id:
                    logging.info("redirect: client_append_entries to leader")
                return None
            else:
                self.client_addr = addr
                return data

        if data["dst_id"] != self.id:
            logging.info("redirect: to " + data["dst_id"])
            self.send(data, self.peers[data["dst_id"]])
            return None
        else:
            return data

    # append entries rpc
    def append_entries(self, data):
        """
        对leader复制日志条目的响应
        :param data:
        :return:
        """
        response = {
            "type": "append_entries_response",
            "src_id": self.id,
            "dst_id": data["src_id"],
            "term": self.current_term,
            "success": False,
        }

        # 如果接收到的消息term比当前节点更小, 拒绝leader
        if data["term"] < self.current_term:
            logging.info("    2. smaller term")
            logging.info("    3. success = False: smaller term")
            logging.info("    4. send append_entries_response to leader " + data["src_id"])
            response["success"] = False
            self.send(response, self.peers[data["src_id"]])
            return

        self.leader_id = data["leader_id"]

        # 不含新日志条目的heartbeat
        if data["entries"] is None:
            logging.info("    4. heartbeat")
            return

        prev_log_index = data["prev_log_index"]
        prev_log_term = data["prev_log_term"]

        local_prev_log_term = self.log.get_log_term(prev_log_index)

        # 如果要求复制的日志与该节点保存的最新一条日志不匹配, 复制失败
        if local_prev_log_term != prev_log_term:
            logging.info("     4. success = False: index not match or term not match")
            logging.info("     5. send append_entries_response to leader " + data["src_id"])
            logging.info("     6. log delete_entries")
            logging.info("     7. log save")

            response["success"] = False
            self.send(response, self.peers[data["src_id"]])
            # 将与leader节点不一致的日志删除
            self.log.delete_entries(prev_log_index)
        else:
            logging.info("    4. success = True")
            logging.info("    5. send append_entries_response to leader " + data["src_id"])
            logging.info("    6. log append_entries")
            logging.info("    7. log save")

            # 发送追加成功的消息
            # 并添加日志条目
            response["success"] = True
            self.send(response, self.peers[data["src_id"]])
            self.log.append_entries(prev_log_index, data["entries"])

            # 如果已提交日志比当前节点已提交日志的索引更大
            # 当前节点更新提交日志索引
            leader_commit = data["leader_commit"]
            if leader_commit > self.commit_index:
                commit_index = min(leader_commit, self.log.last_log_index)
                logging.info("    8. commit_index " + str(commit_index))

        return

    def request_vote(self, data):
        """
        响应request_vote消息
        :param data:
        :return:
        """
        response = {
            "type": "request_vote_response",
            "src_id": self.id,
            "dst_id": data["src_id"],
            "term": self.current_term,
            "vote_granted": False
        }

        # 如果请求投票的candidate term比当前节点更小, 拒绝投票
        if data["term"] < self.current_term:
            logging.info("    2. smaller term")
            logging.info("    3. success = False")
            logging.info("    4. send request_vote_response to candidate " + data["src_id"])
            response["vote_granted"] = False
            self.send(response, self.peers[data['src_id']])
            return

        logging.info("    2. same term")
        candidate_id = data["candidate_id"]
        last_log_index = data["last_log_index"]
        last_log_term = data["last_log_term"]

        # 当节点未投票或已经投票给发来请求的节点
        if self.voted_for is None or self.voted_for == candidate_id:
            # 判断发送请求的节点日志是否比本节点的日志更新
            if last_log_index >= self.log.last_log_index and last_log_term >= self.log.last_log_term:
                self.voted_for = data["src_id"]
                # 持久化保存
                self.save()
                # 同意投票
                response["vote_granted"] = True
                self.send(response, self.peers[data["src_id"]])
                logging.info("    3. success = True: candidate log is newer")
                logging.info("    4. send request_vote_response to candidate " + data["src_id"])
            # 否则, 拒绝节点的投票请求
            else:
                self.voted_for = None
                self.save()
                response["vote_granted"] = False
                self.send(response, self.peers[data["src_id"]])
                logging.info("    3. success = False: candidate log is older")
                logging.info("    4. send request_vote_response to candidate " + str(data["src_id"]))
        # 节点已经投票过
        else:
            response["vote_granted"] = False
            self.save()
            self.send(response, self.peers[data["src_id"]])
            logging.info("    3. success = False: has voted for " + self.voted_for)
            logging.info("    4. send request_vote_response to candidate " + str(data["src_id"]))

        return

    def all_do(self, data):
        logging.info("----------------all----------------")

        # 应用已提交的日志
        if self.commit_index > self.last_applied:
            self.last_applied = self.commit_index
            logging.info("all: 1. last_applied = " + str(self.last_applied))

        if data is None:
            return

        if data["type"] == "client_append_entries":
            # follower节点不响应client的请求
            # TODO 重定向到leader
            return

        # 若接收到一个从更新的节点发来的消息, 退化为follower并重置选举状态
        if data["term"] > self.current_term:
            logging.info("all: 1. bigger term")
            logging.info("     2. become follower")
            self.role = "follower"
            self.current_term = data["term"]
            self.voted_for = data["src_id"]
            self.save()

        return

    def follower_do(self, data):
        """
        follower节点应完成的功能
        :param data:
        :return:
        """
        logging.info("----------------follower----------------")

        t = time.time()
        if data is not None:

            # 处理append_entries消息
            if data["type"] == "append_entries":
                logging.info("follower: 1. recv append_entries from leader " + data["src_id"])

                # 若term合法,  则接受这条日志
                if data["term"] == self.current_term:
                    logging.info("          2. same term")
                    logging.info("          3. reset next_election_time")
                    # 重置选举超时时间
                    self.next_leader_election_time = t + random.randint(*self.wait_ms)
                    # 添加日志
                    self.append_entries(data)

            # 处理选举消息
            elif data["type"] == "request_vote":
                logging.info("follower: 1. recv request_vote from candidate " + data["src_id"])
                self.request_vote(data)

        # 经过选举超时间隔, 未收到leader消息, 转为candidate
        if t > self.next_leader_election_time:
            logging.info("follower: 1. become candidate")
            self.next_leader_election_time = t + random.randint(*self.wait_ms)
            self.role = "candidate"
            # 给自己投票
            self.current_term += 1
            self.voted_for = self.id
            self.save()
            self.vote_ids = {_id: 0 for _id in self.peers}

        return

    def candidate_do(self, data):
        """
        candidate节点完成的功能
        :param data:
        :return:
        """
        logging.info("----------------candidate----------------")
        t = time.time()
        # 向所有节点广播request_vote
        for dst_id in self.peers:
            if self.vote_ids[dst_id] == 0:
                logging.info("candidate: 1. send request_vote to peer " + dst_id)
                request = {
                    "type": "request_vote",
                    "src_id": self.id,
                    "dst_id": dst_id,
                    "term": self.current_term,
                    "candidate_id": self.id,
                    "last_log_index": self.log.last_log_index,
                    "last_log_term": self.log.last_log_term,
                }
                # 发送请求选举
                self.send(request, self.peers[dst_id])

        if data is not None and data["term"]:
            # 处理response
            if data["type"] == "request_vote_response":
                logging.info("candidate: 1. recv request_vote_response from follower " + data["src_id"])

                self.vote_ids[data["src_id"]] = data["vote_granted"]
                vote_count = sum(list(self.vote_ids.values()))

                # 节点获得多数派支持即成为新的leader
                if vote_count >= len(self.peers) // 2:
                    logging.info("    2. become leader")
                    self.role = "leader"
                    self.voted_for = None
                    self.save()
                    self.next_heartbeat_time = 0
                    # 为每一个follower初始化下一条推送的日志索引和匹配的日志索引
                    self.next_index = {_id: self.log.last_log_index + 1 for _id in self.peers}
                    self.match_index = {_id: 0 for _id in self.peers}
                    return

            # 如果收到其他节点声称自己当选为leader
            elif data["type"] == "append_entries":
                logging.info("candidate: 1. recv append_entries from leader " + str("src_id"))
                logging.info("           2. become follower")
                self.next_leader_election_time = t + random.randint(*self.wait_ms)
                self.role = "follower"
                self.voted_for = None
                self.save()
                return

        # 本轮选举未选出leader, 重新开始新一轮选举
        if t > self.next_leader_election_time:
            logging.info("candidate: 1. leader_election timeout")
            logging.info("           2. become candidate")
            self.next_leader_election_time = t + random.randint(*self.wait_ms)
            self.role = "candidate"
            self.current_term += 1
            self.voted_for = self.id
            self.save()
            self.vote_ids = {_id: 0 for _id in self.peers}
            return

    def leader_do(self, data):
        """
        leader 必须实现的功能
        :param data:
        :return:
        """
        logging.info("----------------leader----------------")
        t = time.time()

        # heartbeat信号时间必须远小于election timeout
        if t > self.next_heartbeat_time:
            self.next_heartbeat_time = t + random.randint(0, 5)

            # 广播append entries消息
            for dst_id in self.peers:
                logging.info("leader: 1. send append_entries to peer " + dst_id)

                request = {
                    "type": "append_entries",
                    "src_id": self.id,
                    "dst_id": dst_id,
                    "term": self.current_term,
                    "leader_id": self.id,
                    "prev_log_index": self.next_index[dst_id] - 1,
                    "prev_log_term": self.log.get_log_term(self.next_index[dst_id] - 1),
                    "entries": self.log.get_entries(self.next_index[dst_id]),
                    "leader_commit": self.commit_index,
                }

                self.send(request, self.peers[dst_id])

        # 处理client请求
        if data is not None and data["type"] == "client_append_entries":
            data["term"] = self.current_term
            # 追加日志
            self.log.append_entries(self.log.last_log_index, [data])

            logging.info("leader: 1. recv append_entries from client")
            logging.info("        2. log append_entries")
            logging.info("        3. log save")
            return

        # 处理append_entries response
        if data is not None and data["term"] == self.current_term:
            if data["type"] == "append_entries_response":
                logging.info("leader: 1. recv append_entries_response from follower " + data["src_id"])
                # 追加日志不成功
                if not data["success"]:
                    # next_index - 1, 重试
                    self.next_index[data["src_id"]] -= 1
                    logging.info("leader: 1. recv append_entries_response from follower " + data["src_id"])
                else:
                    # 日志复制成功, 匹配日志索引增加
                    self.match_index[data["src_id"]] = self.next_index[data["src_id"]]
                    self.next_index[data["src_id"]] = self.log.last_log_index + 1
                    self.match_index[data['src_id']] = self.next_index[data['src_id']]
                    self.next_index[data['src_id']] = self.log.last_log_index + 1
                    logging.info("        2. success = True")
                    logging.info(
                        "        3. match_index = " + str(self.match_index[data['src_id']]) + " next_index = " + str(
                            self.next_index[data['src_id']]))

        while True:
            N = self.commit_index + 1

            count = 0
            for _id in self.peers:
                if self.match_index[_id] >= N:
                    count += 1
                # 若日志已经被复制到多数机器上, 则提交这条日志
                if count >= len(self.peers) // 2:
                    self.commit_index = N
                    logging.info("leader: 1. commit + 1")
                    # 通知client这条日志已经被提交
                    if self.client_addr:
                        response = {
                            "index": self.commit_index,
                        }
                        self.send(response, (self.client_addr[0], 10000))

                    break

            else:
                logging.info("leader: 2. commit = " + str(self.match_index))
                break

    def run(self):
        while True:
            try:
                try:
                    data, addr = self.recv()
                except Exception as e:
                    data, addr = None, None

                data = self.redirect(data, addr)

                self.all_do(data)

                if self.role == "follower":
                    self.follower_do(data)

                if self.role == "candidate":
                    self.candidate_do(data)

                if self.role == "leader":
                    self.leader_do(data)
            except Exception as e:
                logging.info(e)
                break

        self.ss.close()
        self.cs.close()
