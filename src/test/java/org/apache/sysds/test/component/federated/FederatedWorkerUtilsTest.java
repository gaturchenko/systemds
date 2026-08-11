/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sysds.test.component.federated;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.ServerSocket;

import org.apache.sysds.test.FederatedWorkerUtils;
import org.junit.Test;

/**
 * Tests for the readiness probe that blocks until a federated worker accepts connections.
 */
public class FederatedWorkerUtilsTest {

	/** Round-trip time the probe should tolerate without giving up, in ms. */
	private static final int TOLERATED_HANDSHAKE_MS = 1000;

	@Test
	public void attemptBudgetCoversADelayedHandshake() {
		// with the sufficient time budget, a single attempt must be allowed to outlast a slow handshake
		assertTrue("per-attempt connect budget is too small to complete a delayed TCP handshake",
			FederatedWorkerUtils.attemptTimeout(Long.MAX_VALUE) >= TOLERATED_HANDSHAKE_MS);
	}

	@Test
	public void attemptBudgetIsCappedByRemainingTime() {
		assertEquals(5, FederatedWorkerUtils.attemptTimeout(5));
		assertEquals(1, FederatedWorkerUtils.attemptTimeout(1));
	}

	@Test
	public void attemptBudgetIsZeroWhenOutOfTime() {
		// 0 must only mean 'do not attempt', since `Socket.connect` reads a timeout of 0 as infinite
		assertEquals(0, FederatedWorkerUtils.attemptTimeout(0));
		assertEquals(0, FederatedWorkerUtils.attemptTimeout(-1));
		assertEquals(0, FederatedWorkerUtils.attemptTimeout(Long.MIN_VALUE));
	}

	@Test
	public void attemptBudgetIsNeverZeroWhileTimeIsLeft() {
		for(long remaining = 1; remaining < 10000; remaining += 7)
			assertTrue("a positive remaining time must not produce an infinite connect timeout",
				FederatedWorkerUtils.attemptTimeout(remaining) > 0);
	}

	@Test
	public void waitReturnsForAListeningPort() throws IOException {
		try(ServerSocket listening = new ServerSocket(0)) {
			// returns as soon as the port accepts, the timeout is only the upper bound
			FederatedWorkerUtils.waitForWorker(listening.getLocalPort(), 1000);
		}
	}

	@Test
	public void waitFailsFastWhenTheWorkerDied() throws IOException {
		final int port;
		try(ServerSocket closed = new ServerSocket(0)) {
			port = closed.getLocalPort();
		}
		final long t0 = System.currentTimeMillis();
		try {
			FederatedWorkerUtils.waitForWorker(port, 1000, () -> false, "worker");
			fail("expected the wait to report the dead worker");
		}
		catch(RuntimeException e) {
			assertTrue(e.getMessage(), e.getMessage().contains("died before becoming ready"));
			// must not sit out the timeout, which is reduced to a minute
			assertTrue("the dead worker was not reported promptly", System.currentTimeMillis() - t0 < 10000);
		}
	}
}
