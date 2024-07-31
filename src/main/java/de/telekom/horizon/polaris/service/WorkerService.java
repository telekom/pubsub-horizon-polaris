// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.polaris.service;

import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.cluster.MembershipListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.lock.FencedLock;
import com.hazelcast.map.IMap;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Service
public class WorkerService implements MembershipListener {

    private final HazelcastInstance hazelcastInstance;

    private final FencedLock globalLock;

    private final ApplicationEventPublisher applicationEventPublisher;

    private final IMap<String, String> claims;

    public WorkerService(HazelcastInstance hazelcastInstance, ApplicationEventPublisher applicationEventPublisher) {
        this.hazelcastInstance = hazelcastInstance;
        this.applicationEventPublisher = applicationEventPublisher;
        this.hazelcastInstance.getCluster().addMembershipListener(this);
        this.globalLock = hazelcastInstance.getCPSubsystem().getLock("polaris-lock");
        this.claims = hazelcastInstance.getMap("polaris-member-claims");
    }

    public boolean tryClaim(String key) {
        var uuid = hazelcastInstance.getCluster().getLocalMember().getUuid().toString();

        return claims.computeIfAbsent(key, k -> uuid).equals(uuid);
    }

    public void removeClaim(String key) {
        claims.computeIfPresent(key, (k, v) -> null);
    }

    public void removeMemberClaims(Member member) {
        claims.entrySet().removeIf(entry -> entry.getValue().equals(member.getUuid().toString()));
    }

    public boolean tryGlobalLock() {
        return globalLock.tryLock ( 10, TimeUnit.SECONDS );
    }

    public void globalUnlock() {
        globalLock.unlock();
    }

    @Override
    public void memberAdded(MembershipEvent membershipEvent) {
        applicationEventPublisher.publishEvent(membershipEvent);
    }

    @Override
    public void memberRemoved(MembershipEvent membershipEvent) {
        removeMemberClaims(membershipEvent.getMember());
        applicationEventPublisher.publishEvent(membershipEvent);
    }
}
