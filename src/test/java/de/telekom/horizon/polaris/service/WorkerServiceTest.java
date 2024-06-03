// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.polaris.service;

import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPSubsystem;
import com.hazelcast.cp.lock.FencedLock;
import com.hazelcast.map.IMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.ApplicationEventPublisher;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class WorkerServiceTest {

    @Mock
    HazelcastInstance hazelcastInstance;

    @Mock
    ApplicationEventPublisher applicationEventPublisher;

    @Mock
    FencedLock globalLock;

    @Mock
    IMap claims;

    WorkerService workerServiceSpy;

    private final static String TEST_UUID = "f33fb884-8928-499a-9dd7-a32ad634b4c5";

    @BeforeEach
    void init() {
        var cluster = Mockito.mock(Cluster.class);
        var cpSubsystem = Mockito.mock(CPSubsystem.class);

        when(hazelcastInstance.getMap("polaris-member-claims")).thenReturn(claims);
        when(hazelcastInstance.getCluster()).thenReturn(cluster);
        when(cpSubsystem.getLock(any())).thenReturn(globalLock);
        when(hazelcastInstance.getCPSubsystem()).thenReturn(cpSubsystem);

        workerServiceSpy = Mockito.spy(new WorkerService(hazelcastInstance, applicationEventPublisher));
    }


    @Test
    void testTryGlobalLock() {
        workerServiceSpy.tryGlobalLock();
        verify(globalLock).tryLock ( 10, TimeUnit.SECONDS );
    }

    @Test
    void testGlobalUnlock() {
        workerServiceSpy.globalUnlock();
        verify(globalLock).unlock();
    }

    @Test
    void testTryClaim() {
        var member = Mockito.mock(Member.class);
        when(member.getUuid()).thenReturn(UUID.fromString(TEST_UUID));
        when(hazelcastInstance.getCluster().getLocalMember()).thenReturn(member);

        var realMap = new ConcurrentHashMap<String, String>();

        when(claims.computeIfAbsent(any(), any())).thenAnswer(input -> realMap.computeIfAbsent(input.getArgument(0), k -> TEST_UUID));

        var key = "foobar";
        workerServiceSpy.tryClaim(key);

        assertThat(realMap.get(key)).isEqualTo(TEST_UUID);
    }

    @Test
    void testRemoveClaim() {
        var key = "foobar";

        var realMap = new ConcurrentHashMap<String, String>();
        realMap.put(key, TEST_UUID);

        when(claims.computeIfPresent(any(), any())).thenAnswer(input -> realMap.computeIfPresent(input.getArgument(0), (k, v) -> null));

        assertThat(realMap.get(key)).isEqualTo(TEST_UUID);
        assertThat(realMap.get(key)).isNotNull();

        workerServiceSpy.removeClaim(key);

        assertThat(realMap.get(key)).isNull();
        assertThat(realMap.isEmpty()).isTrue();
    }

    @Test
    void testRemoveMemberClaims() {
        var subscriptionId1 = "foo";
        var subscriptionId2 = "bar";
        var subscriptionId3 = "xyz";

        var realMap = new ConcurrentHashMap<String, String>();
        realMap.put(subscriptionId1, TEST_UUID);
        realMap.put(subscriptionId2, TEST_UUID);
        realMap.put(subscriptionId3, "123");

        var member = Mockito.mock(Member.class);
        when(member.getUuid()).thenReturn(UUID.fromString(TEST_UUID));

        when(claims.entrySet()).thenReturn(realMap.entrySet());

        workerServiceSpy.removeMemberClaims(member);

        verify(claims, times(2)).remove(any());
        verify(claims, times(1)).remove(subscriptionId1);
        verify(claims, times(1)).remove(subscriptionId2);
        verify(claims, never()).remove(subscriptionId3);
    }

    @Test
    void testMemberAdded() {
        var membershipEvent = Mockito.mock(MembershipEvent.class);

        workerServiceSpy.memberAdded(membershipEvent);

        verify(applicationEventPublisher, times(1)).publishEvent(membershipEvent);
    }

    @Test
    void testMemberRemoved() {
        var membershipEvent = Mockito.mock(MembershipEvent.class);
        var member = Mockito.mock(Member.class);

        when(membershipEvent.getMember()).thenReturn(member);

        workerServiceSpy.memberRemoved(membershipEvent);

        verify(workerServiceSpy, times(1)).removeMemberClaims(member);
        verify(applicationEventPublisher, times(1)).publishEvent(membershipEvent);
    }
}
