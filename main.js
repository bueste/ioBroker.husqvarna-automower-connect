'use strict';

/*
 * Created with @iobroker/create-adapter v2.3.0
 */

// The adapter-core module gives you access to the core ioBroker functions
// you need to create an adapter
const utils = require('@iobroker/adapter-core');

// Load your modules here, e.g.:
const axios = require('axios');
const WebSocket = require('ws');

// variables
const isValidApplicationCredential = /^[a-zA-Z0-9]{8}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{12}$/; // format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx (anchored: without ^/$ any string merely *containing* this pattern would pass)

class HusqvarnaAutomower extends utils.Adapter {
	/**
	 * @param {Partial<utils.AdapterOptions>} [options={}]
	 */
	constructor(options) {
		super({
			...options,
			name: 'husqvarna-automower-connect',
		});
		this.on('ready', this.onReady.bind(this));
		this.on('stateChange', this.onStateChange.bind(this));
		this.on('unload', this.onUnload.bind(this));

		this.wss = null;

		this.access_token = null;
		this.mowerData = null;

		this.firstStart = true;
		this.statisticsPollInProgress = false;

		this.autoRestartTimeout = null;
		this.ping = null;
		// exponential backoff for WebSocket reconnects (autoRestart()) - reset to the base delay on every
		// successful (re-)connection, see the 'open' handler in connectToWS()
		this.wsReconnectDelay = 5000;

		this.statisticsInterval = null;

		this.numberOfSchedules = 0;
		this.capabilities = [];
	}

	/**
	 * Is called when databases are connected and adapter received configuration.
	 */
	// One-time (well, cheap-and-idempotent-every-startup) fix for a set of known role/type mistakes
	// present in objects created by versions before 1.0.3 (see CHANGELOG). setObjectNotExistsAsync()
	// never touches an object that already exists, so simply updating the adapter does not correct
	// objects an already-running installation had already created - this actively force-corrects them
	// via extendObjectAsync() instead. Only touches an object if its CURRENT role still matches the
	// known-bad value, so it never clobbers anything a user might have customized in the meantime.
	async migrateObjectRoles() {
		try {
			const objects = await this.getAdapterObjectsAsync();
			const fixes = [
				{ suffix: /\.ACTIONS\.HEADLIGHT$/, badRole: 'value', common: { role: 'state' } },
				{ suffix: /\.ACTIONS\.schedule\.\d+\.(start|duration)$/, badRole: 'value', common: { role: 'level' } },
				{ suffix: /\.ACTIONS\.schedule\.\d+\.(monday|tuesday|wednesday|thursday|friday|saturday|sunday)$/, badRole: 'value', common: { role: 'switch' } },
				{ suffix: /\.messages\.messages$/, badType: 'array', common: { type: 'string' } },
				{ suffix: /\.system\.id$/, badRole: 'info.id', common: { role: 'text' } },
				{ suffix: /\.system\.type$/, badRole: 'info.type', common: { role: 'text' } },
				{ suffix: /\.system\.serialNumber$/, badRole: 'info.serialnumber', common: { role: 'info.serial' } },
			];

			let fixedCount = 0;
			for (const id of Object.keys(objects)) {
				const obj = objects[id];
				if (!obj || obj.type !== 'state' || !obj.common) {
					continue;
				}
				for (const fix of fixes) {
					if (!fix.suffix.test(id)) {
						continue;
					}
					const roleMatches = fix.badRole === undefined || obj.common.role === fix.badRole;
					const typeMatches = fix.badType === undefined || obj.common.type === fix.badType;
					if (roleMatches && typeMatches) {
						await this.extendObjectAsync(id, { common: fix.common });
						fixedCount++;
					}
					break; // each id matches at most one fix pattern
				}
			}
			if (fixedCount > 0) {
				this.log.info(`Migration: corrected role/type on ${fixedCount} existing object(s) created by a version before 1.0.3.`);
			}
		} catch (e) {
			// Never let a migration failure block adapter startup - worst case the objects stay
			// as they were, which is the same situation as before this migration existed.
			this.log.warn(`Migration of object roles/types failed (non-fatal, adapter will continue starting): ${e}`);
		}
	}

	async onReady() {
		// Initialize your adapter here
		this.log.info('starting adapter "husqvarna-automower"...');

		// One-time migration: setObjectNotExistsAsync() (used throughout this adapter to create
		// states) only creates an object if it does NOT already exist yet - it never updates an
		// already-existing object. Installations that were running before 1.0.3 therefore kept the
		// incorrect roles/types fixed in that release (see CHANGELOG) forever, even after updating,
		// since the objects already existed. This forces those specific, known-bad objects to the
		// corrected values on every startup (extendObjectAsync is cheap/idempotent once corrected).
		await this.migrateObjectRoles();

		// Reset the connection indicator during startup
		this.setState('info.connection', false, true);

		// The adapters config (in the instance object everything under the attribute "native") is accessible via this.config:
		// NOTE: never log the actual applicationKey/applicationSecret values, even at debug level - this adapter's
		// own README tells users to enable debug logging and attach the logfile when filing a GitHub issue, and
		// the Application Secret in particular is a credential, not just an identifier.
		this.log.debug(`config.applicationKey: ${this.config.applicationKey ? '[set]' : '[missing]'}`);
		this.log.debug(`config.applicationSecret: ${this.config.applicationSecret ? '[set]' : '[missing]'}`);
		this.log.debug(`config.statisticsInterval: ${this.config.statisticsInterval}`);

		// check applicationKey
		if (!isValidApplicationCredential.test(this.config.applicationKey)) {
			this.log.error('"Application Key" is not valid (allowed format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx) (ERR_#001)');
			return;
		}
		// check applicationSecret
		if (!isValidApplicationCredential.test(this.config.applicationSecret)) {
			this.log.error('"Application Secret" is not valid (allowed format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx) (ERR_#002)');
			return;
		}
		// check statisticsInterval
		// NOTE: this used "&&" instead of "||", which made the check permanently unreachable (a number can never be
		// both < 5 and > 10080 at the same time) - any configured value, including 0 or negative, silently passed.
		if (Number(this.config.statisticsInterval) < 5 || Number(this.config.statisticsInterval) > 10080) {
			this.log.error('"Time interval to retrieve statistical values" is not valid (5 <= t <= 10080 minutes) (ERR_#003)');
			return;
		}
		this.log.debug('The configuration has been checked successfully. Trying to connect "Automower Connect API"...');

		try {
			// get Husqvarna access_token
			await this.getAccessToken();

			// get data from husqvarna API
			await this.getMowerData();

			// create objects
			await this.createObjects(this.mowerData);

			// fill in states
			await this.fillObjects(this.mowerData);

			// get message/error history (non-fatal if it fails, see getAndFillMowerMessages())
			await this.getAndFillMowerMessages();

			// establish WebSocket connection
			await this.connectToWS();

			// get statistics
			this.statisticsInterval = this.setInterval(async () => {
				// guard against overlapping runs: with a short statisticsInterval and a slow/degraded connection,
				// a cycle could still be in flight when the next one is due to start, which risked concurrent
				// getMowerData()/fillObjects() calls racing each other on the same state tree.
				if (this.statisticsPollInProgress) {
					this.log.debug('[statisticsInterval]: previous poll still in progress, skipping this tick.');
					return;
				}
				this.statisticsPollInProgress = true;
				try {
					await this.getMowerData();
					await this.fillObjects(this.mowerData);
					// NOTE: message history is deliberately NOT re-fetched here on every tick. New messages already
					// arrive live via the WebSocket "message" push event (see connectToWS()); polling the full
					// GET .../messages list on every statistics cycle would double the request volume against
					// Husqvarna's 10 000 requests/month budget for anyone using a short statisticsInterval, for a
					// list that in the steady state rarely changes between polls anyway. It is still refreshed on
					// adapter startup and on-demand via ACTIONS.REFRESHSTATISTICS.
				} catch (error) {
					this.log.debug(`${error} (ERR_#015)`);
				} finally {
					this.statisticsPollInProgress = false;
				}
			}, this.config.statisticsInterval * 60000); // max. 10000 requests/month; (31d*24h*60min*60s*1000ms)/10000requests/month = 267840ms = 4.46min
		} catch (error) {
			this.log.error(`${error} (ERR_#004)`);
		}
	}

	/**
	 * Returns a deep copy of the given value with known-sensitive fields masked. Used everywhere before a value is
	 * passed to this.log.debug(). This matters because this adapter's own README explicitly tells users to enable
	 * debug logging and attach the logfile when filing a GitHub issue - without redaction, the Application Secret,
	 * Application Key and the live OAuth access token would end up in plaintext in every debug log line that logs
	 * an axios request/response, and very likely in a publicly posted bug report sooner or later.
	 *
	 * @param {unknown} value
	 * @returns {unknown} a deep copy of value with sensitive fields replaced by '***redacted***'
	 */
	redact(value) {
		const SENSITIVE_KEYS = new Set(['authorization', 'x-api-key', 'access_token', 'refresh_token', 'client_secret', 'client_id', 'applicationkey', 'applicationsecret']);
		const seen = new WeakSet();
		const walk = input => {
			if (input === null || typeof input !== 'object') {
				return input;
			}
			if (seen.has(input)) {
				return '[circular]';
			}
			seen.add(input);
			if (Array.isArray(input)) {
				return input.map(walk);
			}
			const out = {};
			for (const [key, val] of Object.entries(input)) {
				if (SENSITIVE_KEYS.has(key.toLowerCase())) {
					out[key] = '***redacted***';
				} else if (key === 'data' && typeof val === 'string' && (val.includes('client_secret') || val.includes('client_id'))) {
					// axios request body for the "get access token" call is a raw x-www-form-urlencoded string,
					// not an object, so the key-based masking above does not apply to it - mask it explicitly.
					out[key] = val.replace(/client_secret=[^&]*/i, 'client_secret=***redacted***').replace(/client_id=[^&]*/i, 'client_id=***redacted***');
				} else {
					out[key] = walk(val);
				}
			}
			return out;
		};
		return walk(value);
	}

	/**
	 * Consistent, redacted debug logging for a failed axios request. Factored out because five call sites each had
	 * a hand-written, near-identical copy of this block - which had already drifted out of sync at least once (a
	 * copy-pasted context label pointing at the wrong function name, see git history).
	 *
	 * @param {string} context - short label identifying the calling function, e.g. 'getMowerData'
	 * @param {unknown} error - the error caught from a failed axios request
	 */
	logAxiosError(context, error) {
		if (error.response) {
			// The request was made and the server responded with a status code that falls out of the range of 2xx
			this.log.debug(`[${context}]: HTTP status response: ${error.response.status}; headers: ${JSON.stringify(this.redact(error.response.headers))}; data: ${JSON.stringify(this.redact(error.response.data))}`);
		} else if (error.request) {
			// The request was made but no response was received - error.request is an instance of XMLHttpRequest in
			// the browser and an instance of http.ClientRequest in node.js
			this.log.debug(`[${context}]: error request: ${error}`);
		} else {
			// Something happened in setting up the request that triggered an Error
			this.log.debug(`[${context}]: error message: ${error.message}`);
		}
		this.log.debug(`[${context}]: error.config: ${JSON.stringify(this.redact(error.config))}`);
	}

	// https://developer.husqvarnagroup.cloud/apis/authentication-api#readme
	async getAccessToken() {
		await axios({
			method: 'POST',
			url: 'https://api.authentication.husqvarnagroup.dev/v1/oauth2/token',
			data: `grant_type=client_credentials&client_id=${this.config.applicationKey}&client_secret=${this.config.applicationSecret}`,
		})
			.then(response => {
				this.log.debug(`[getAccessToken]: HTTP status response: ${response.status} ${response.statusText}; config: ${JSON.stringify(this.redact(response.config))}; headers: ${JSON.stringify(this.redact(response.headers))}; data: ${JSON.stringify(this.redact(response.data))}`);

				this.access_token = response.data.access_token;

				if (this.firstStart === true) {
					this.log.info('"Husqvarna Authentication API Access token" received.');
				} else {
					this.log.debug('"Husqvarna Authentication API Access token" received.');
				}
			})
			.catch(error => {
				this.logAxiosError('getAccessToken', error);
				throw new Error('"Automower Connect API" not reachable. (ERR_#005)');
			});
	}

	// https://developer.husqvarnagroup.cloud/apis/automower-connect-api#readme
	async getMowerData() {
		await axios({
			method: 'GET',
			url: 'https://api.amc.husqvarna.dev/v1/mowers',
			headers: {
				Authorization: `Bearer ${this.access_token}`,
				'X-Api-Key': this.config.applicationKey,
				'Authorization-Provider': 'husqvarna',
			},
		})
			.then(async response => {
				this.log.debug(`[getMowerData]: HTTP status response: ${response.status} ${response.statusText}; config: ${JSON.stringify(this.redact(response.config))}; headers: ${JSON.stringify(this.redact(response.headers))}; data: ${JSON.stringify(response.data)}`);

				this.mowerData = response.data;
				this.log.debug(`[getMowerData]: response.data: ${JSON.stringify(response.data)}`);
			})
			.catch(error => {
				this.logAxiosError('getMowerData', error);
				throw new Error('"Automower Connect API" not reachable. (ERR_#006)');
			});
	}

	// https://developer.husqvarnagroup.cloud/apis/automower-connect-api#readme (GET .../messages)
	// Fetches and stores the diagnostic/error message history for every known mower. Non-fatal on error:
	// message history is supplementary information and must never block the core status update cycle.
	async getAndFillMowerMessages() {
		if (!this.mowerData || !Array.isArray(this.mowerData.data)) {
			return;
		}
		for (const mower of this.mowerData.data) {
			if (mower.type !== 'mower') {
				continue;
			}
			await axios({
				method: 'GET',
				url: `https://api.amc.husqvarna.dev/v1/mowers/${mower.id}/messages`,
				headers: {
					Authorization: `Bearer ${this.access_token}`,
					'X-Api-Key': this.config.applicationKey,
					'Authorization-Provider': 'husqvarna',
				},
			})
				.then(response => {
					this.log.debug(`[getAndFillMowerMessages]: HTTP status response: ${response.status} ${response.statusText}; data: ${JSON.stringify(response.data)}`);
					const messages = (response.data && response.data.data && response.data.data.attributes && response.data.data.attributes.messages) || [];
					this.setState(`${mower.id}.messages.messages`, { val: JSON.stringify(messages), ack: true });
					if (messages.length > 0) {
						this.setState(`${mower.id}.messages.lastTime`, { val: messages[0].time, ack: true });
						this.setState(`${mower.id}.messages.lastCode`, { val: messages[0].code, ack: true });
						this.setState(`${mower.id}.messages.lastSeverity`, { val: messages[0].severity, ack: true });
						this.setState(`${mower.id}.messages.lastLatitude`, { val: messages[0].latitude, ack: true });
						this.setState(`${mower.id}.messages.lastLongitude`, { val: messages[0].longitude, ack: true });
					}
				})
				.catch(error => {
					this.logAxiosError('getAndFillMowerMessages', error);
					// intentionally not re-thrown: message history is supplementary and must not block core status updates
				});
		}
	}

	// https://github.com/ioBroker/ioBroker.docs/blob/master/docs/en/dev/objectsschema.md
	// https://github.com/ioBroker/ioBroker/blob/master/doc/STATE_ROLES.md#state-roles
	async createObjects(mowerData) {
		// this.log.debug(`[createObjects]: listMowers: ${JSON.stringify(listMowers)}`);

		this.log.debug(`[createObjects]: start objects creation for ${Object.keys(mowerData.data).length} device${Object.keys(mowerData.data).length > 1 ? 's' : ''}...`);
		if (Object.keys(mowerData.data).length !== 0) {
			for (let i = 0; i < Object.keys(mowerData.data).length; i++) {
				if (mowerData.data[i].type === 'mower') {
					// create device
					await this.setObjectNotExistsAsync(mowerData.data[i].id, {
						type: 'device',
						common: {
							name: mowerData.data[i].attributes.system.model,
							// icon: deviceIcon
						},
						native: {},
					});

					// create channel "system"
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.system`, {
						type: 'channel',
						common: {
							name: 'System information about an Automower',
						},
						native: {},
					});
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.system.type`, {
						type: 'state',
						common: {
							name: 'Device type',
							type: 'string',
							role: 'text',
							read: true,
							write: false,
						},
						native: {},
					});
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.system.id`, {
						type: 'state',
						common: {
							name: 'Device ID',
							type: 'string',
							role: 'text',
							read: true,
							write: false,
						},
						native: {},
					});

					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.system.name`, {
						type: 'state',
						common: {
							name: 'The name given to the Automower by the user when pairing the Automower',
							type: 'string',
							role: 'info.name',
							read: true,
							write: false,
						},
						native: {},
					});
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.system.model`, {
						type: 'state',
						common: {
							name: 'The model name of the Automower',
							type: 'string',
							role: 'info.model',
							read: true,
							write: false,
						},
						native: {},
					});
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.system.serialNumber`, {
						type: 'state',
						common: {
							name: 'The serial number for the Automower',
							type: 'number',
							role: 'info.serial',
							read: true,
							write: false,
						},
						native: {},
					});

					// create channel "battery"
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.battery`, {
						type: 'channel',
						common: {
							name: 'Information about the battery in the Automower',
						},
						native: {},
					});
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.battery.batteryPercent`, {
						type: 'state',
						common: {
							name: 'The current battery level percentage',
							type: 'number',
							role: 'value.battery',
							min: 0,
							max: 100,
							unit: '%',
							read: true,
							write: false,
						},
						native: {},
					});

					// create channel "capabilities"
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.capabilities`, {
						type: 'channel',
						common: {
							name: 'Information about what capabilities the Automower has',
						},
						native: {},
					});

					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.capabilities.canConfirmError`, {
						type: 'state',
						common: {
							name: 'If the Automower supports the command confirm error. The error also needs to be confirmable.',
							type: 'boolean',
							role: 'state',
							read: true,
							write: false,
						},
						native: {},
					});
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.capabilities.headlights`, {
						type: 'state',
						common: {
							name: 'If the Automower supports headlights. If false, no headlights are available.',
							type: 'boolean',
							role: 'state',
							read: true,
							write: false,
						},
						native: {},
					});
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.capabilities.position`, {
						type: 'state',
						common: {
							name: 'If the Automower supports GPS position. If false, no positions are available.',
							type: 'boolean',
							role: 'state',
							read: true,
							write: false,
						},
						native: {},
					});
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.capabilities.stayOutZones`, {
						type: 'state',
						common: {
							name: 'If the Automower supports stay-out zones. If false, no stay-out zones are available.',
							type: 'boolean',
							role: 'state',
							read: true,
							write: false,
						},
						native: {},
					});
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.capabilities.workAreas`, {
						type: 'state',
						common: {
							name: 'If the Automower supports work areas. If false, no work areas are avalilable.',
							type: 'boolean',
							role: 'state',
							read: true,
							write: false,
						},
						native: {},
					});

					// create channel "mower", see https://developer.husqvarnagroup.cloud/apis/Automower+Connect+API#/status%20description%20and%20error%20codes for descriptions of status and error codes
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.mower`, {
						type: 'channel',
						common: {
							name: 'Information about the mowers current status.',
						},
						native: {},
					});
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.mower.mode`, {
						type: 'state',
						common: {
							name: 'Information about the mowers current mode.',
							type: 'string',
							role: 'state',
							states: {
								MAIN_AREA: 'Mower will mow until low battery. Go home and charge. Leave and continue mowing.',
								DEMO: 'Same as main area, but shorter times. (No blade operation)',
								SECONDARY_AREA: 'Mower will mow until empty battery, or a limited time. When done, it stops in the garden.',
								HOME: 'Mower goes home and parks forever.',
								UNKNOWN: 'Unknown mode.',
							},
							read: true,
							write: false,
						},
						native: {},
					});
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.mower.activity`, {
						type: 'state',
						common: {
							name: 'Information about the mowers current activity',
							type: 'string',
							role: 'state',
							states: {
								UNKNOWN: 'Unknown activity.',
								NOT_APPLICABLE: 'Manual start required in mower.',
								MOWING: 'Mower is mowing lawn. If in demo mode the blades are not in operation.',
								GOING_HOME: 'Mower is going home to the charging station.',
								CHARGING: 'Mower is charging in station due to low battery.',
								LEAVING: 'Mower is leaving the charging station.',
								PARKED_IN_CS: 'Mower is parked in charging station.',
								STOPPED_IN_GARDEN: 'Mower has stopped. Needs manual action to resume.',
							},
							read: true,
							write: false,
						},
						native: {},
					});
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.mower.inactiveReason`, {
						type: 'state',
						common: {
							name: 'Inactive reason',
							type: 'string',
							role: 'state',
							states: {
								NONE: 'No inactive reason.',
								PLANNING: 'The mower is planning a path or a work area.',
								SEARCHING_FOR_SATELLITES: 'Waiting for fix when using EPOS.',
							},
							read: true,
							write: false,
						},
						native: {},
					});
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.mower.state`, {
						type: 'state',
						common: {
							name: 'Information about the mowers current state',
							type: 'string',
							role: 'state',
							states: {
								UNKNOWN: 'Unknown state.',
								NOT_APPLICABLE: 'Not applicable.',
								PAUSED: 'Mower has been paused by user.',
								IN_OPERATION: 'Mower is operating according to selected mode. The activity gives information about what it is currently up to.',
								WAIT_UPDATING: 'Mower is in wait state when updating.',
								WAIT_POWER_UP: 'Mower is in wait state when powering up.',
								RESTRICTED: 'The mower is currently restricted from mowing for some reason. It will continue mowing when the restriction is removed. The activity gives information about what the mower is currently up to.',
								OFF: 'Mower is turned off.',
								STOPPED: 'Mower is stopped, and cannot be started remotely. Start requirements (safety or other) are not fulfilled.',
								ERROR: 'A temporary error has occured. If the error is resolved, the mower will resume operation without user interaction. Typically, this happens when the loop signal is lost. When it comes back, the operation is resumed.',
								FATAL_ERROR: 'A fatal error has occured. Error has to be fixed confirmed to leave this state.',
								ERROR_AT_POWER_UP: 'An error at power up.',
							},
							read: true,
							write: false,
						},
						native: {},
					});
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.mower.workAreaId`, {
						type: 'state',
						common: {
							name: 'Current work area id. If the mower supports work areas and the mower is working on a work area. If no current work area is selected this attribute is not set.',
							type: 'number',
							role: 'state',
							read: true,
							write: false,
						},
						native: {},
					});
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.mower.errorCode`, {
						type: 'state',
						common: {
							name: 'Information about the mowers current error status',
							type: 'number',
							role: 'state',
							states: {
								0: 'Unexpected error',
								1: 'Outside working area',
								2: 'No loop signal',
								3: 'Wrong loop signal',
								4: 'Loop sensor problem, front',
								5: 'Loop sensor problem, rear',
								6: 'Loop sensor problem, left',
								7: 'Loop sensor problem, right',
								8: 'Wrong PIN code',
								9: 'Trapped',
								10: 'Upside down',
								11: 'Low battery',
								12: 'Empty battery',
								13: 'No drive',
								14: 'Mower lifted',
								15: 'Lifted',
								16: 'Stuck in charging station',
								17: 'Charging station blocked',
								18: 'Collision sensor problem, rear',
								19: 'Collision sensor problem, front',
								20: 'Wheel motor blocked, right',
								21: 'Wheel motor blocked, left',
								22: 'Wheel drive problem, right',
								23: 'Wheel drive problem, left',
								24: 'Cutting system blocked',
								25: 'Cutting system blocked',
								26: 'Invalid sub-device combination',
								27: 'Settings restored',
								28: 'Memory circuit problem',
								29: 'Slope too steep',
								30: 'Charging system problem',
								31: 'STOP button problem',
								32: 'Tilt sensor problem',
								33: 'Mower tilted',
								34: 'Cutting stopped - slope too steep',
								35: 'Wheel motor overloaded, right',
								36: 'Wheel motor overloaded, left',
								37: 'Charging current too high',
								38: 'Electronic problem',
								39: 'Cutting motor problem',
								40: 'Limited cutting height range',
								41: 'Unexpected cutting height adj',
								42: 'Limited cutting height range',
								43: 'Cutting height problem, drive',
								44: 'Cutting height problem, curr',
								45: 'Cutting height problem, dir',
								46: 'Cutting height blocked',
								47: 'Cutting height problem',
								48: 'No response from charger',
								49: 'Ultrasonic problem',
								50: 'Guide 1 not found',
								51: 'Guide 2 not found',
								52: 'Guide 3 not found',
								53: 'GPS navigation problem',
								54: 'Weak GPS signal',
								55: 'Difficult finding home',
								56: 'Guide calibration accomplished',
								57: 'Guide calibration failed',
								58: 'Temporary battery problem',
								59: 'Temporary battery problem',
								60: 'Temporary battery problem',
								61: 'Temporary battery problem',
								62: 'Temporary battery problem',
								63: 'Temporary battery problem',
								64: 'Temporary battery problem',
								65: 'Temporary battery problem',
								66: 'Battery problem',
								67: 'Battery problem',
								68: 'Temporary battery problem',
								69: 'Alarm! Mower switched off',
								70: 'Alarm! Mower stopped',
								71: 'Alarm! Mower lifted',
								72: 'Alarm! Mower tilted',
								73: 'Alarm! Mower in motion',
								74: 'Alarm! Outside geofence',
								75: 'Connection changed',
								76: 'Connection NOT changed',
								77: 'Com board not available',
								78: 'Slipped - Mower has Slipped. Situation not solved with moving pattern',
								79: 'Invalid battery combination - Invalid combination of different battery types',
								80: 'Cutting system imbalance --Warning--',
								81: 'Safety function faulty',
								82: 'Wheel motor blocked, rear right',
								83: 'Wheel motor blocked, rear left',
								84: 'Wheel drive problem, rear right',
								85: 'Wheel drive problem, rear left',
								86: 'Wheel motor overloaded, rear right',
								87: 'Wheel motor overloaded, rear left',
								88: 'Angular sensor problem',
								89: 'Invalid system configuration',
								90: 'No power in charging station',
								91: 'Switch cord problem',
								92: 'Work area not valid',
								93: 'No accurate position from satellites',
								94: 'Reference station communication problem',
								95: 'Folding sensor activated',
								96: 'Right brush motor overloaded',
								97: 'Left brush motor overloaded',
								98: 'Ultrasonic Sensor 1 defect',
								99: 'Ultrasonic Sensor 2 defect',
								100: 'Ultrasonic Sensor 3 defect',
								101: 'Ultrasonic Sensor 4 defect',
								102: 'Cutting drive motor 1 defect',
								103: 'Cutting drive motor 2 defect',
								104: 'Cutting drive motor 3 defect',
								105: 'Lift Sensor defect',
								106: 'Collision sensor defect',
								107: 'Docking sensor defect',
								108: 'Folding cutting deck sensor defect',
								109: 'Loop sensor defect',
								110: 'Collision sensor error',
								111: 'No confirmed position',
								112: 'Cutting system major imbalance',
								113: 'Complex working area',
								114: 'Too high discharge current',
								115: 'Too high internal current',
								116: 'High charging power loss',
								117: 'High internal power loss',
								118: 'Charging system problem',
								119: 'Zone generator problem',
								120: 'Internal voltage error',
								121: 'High internal temerature',
								122: 'CAN error',
								123: 'Destination not reachable',
								124: 'Destination blocked',
								125: 'Battery needs replacement',
								126: 'Battery near end of life',
								127: 'Battery problem',
								128: 'Multiple reference stations detected',
								129: 'Auxiliary cutting means blocked',
								130: 'Imbalanced auxiliary cutting disc detected',
								131: 'Lifted in link arm',
								132: 'EPOS accessory missing',
								133: 'Bluetooth com with CS failed',
								134: 'Invalid SW configuration',
								135: 'Radar problem',
								136: 'Work area tampered',
								137: 'High temperature in cutting motor, right',
								138: 'High temperature in cutting motor, center',
								139: 'High temperature in cutting motor, left',
								141: 'Wheel brush motor problem',
								143: 'Accessory power problem',
								144: 'Boundary wire problem',
								701: 'Connectivity problem',
								702: 'Connectivity settings restored',
								703: 'Connectivity problem',
								704: 'Connectivity problem',
								705: 'Connectivity problem',
								706: 'Poor signal quality',
								707: 'SIM card requires PIN',
								708: 'SIM card locked',
								709: 'SIM card not found',
								710: 'SIM card locked',
								711: 'SIM card locked',
								712: 'SIM card locked',
								713: 'Geofence problem',
								714: 'Geofence problem',
								715: 'Connectivity problem',
								716: 'Connectivity problem',
								717: 'SMS could not be sent',
								724: 'Communication circuit board SW must be updated',
							},
							min: 0,
							max: 724,
							read: true,
							write: false,
						},
						native: {},
					});
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.mower.errorCodeTimestamp`, {
						type: 'state',
						common: {
							name: 'Timestamp for the last error code in milliseconds since 1970-01-01T00:00:00 in local time. NOTE! This timestamp is in local time for the mower and is coming directly from the mower.',
							type: 'number',
							role: 'value.time',
							read: true,
							write: false,
						},
						native: {},
					});
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.mower.isErrorConfirmable`, {
						type: 'state',
						common: {
							name: 'If the mower has an errorCode this attribute state if the error is confirmable.',
							type: 'boolean',
							role: 'state',
							read: true,
							write: false,
						},
						native: {},
					});

					// create channel "planner"
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.planner`, {
						type: 'channel',
						common: {
							name: 'Information about the planner. The planner tells when the mower should work.',
						},
						native: {},
					});
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.planner.nextStartTimestamp`, {
						type: 'state',
						common: {
							name: 'Timestamp for the next auto start in milliseconds since 1970-01-01T00:00:00 in local time. If the mower is charging then the value is the estimated time when it will be leaving the charging station. If the value is 0 then the mower should start now. NOTE! This timestamp is in local time for the mower and is coming directly from the mower.',
							type: 'number',
							role: 'value.time',
							read: true,
							write: false,
						},
						native: {},
					});
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.planner.override`, {
						type: 'state',
						common: {
							name: 'The Planner has an override feature, which can be used to override the operation decided by the Calendar. There is room for one override at a time, and it occurs from now and for a duration of time.',
							type: 'string',
							role: 'state',
							states: {
								NOT_ACTIVE: 'Not active',
								FORCE_PARK: 'Force park until next start means that no more mowing will be done within the current task. Operation will be resumed at the start of the next task instead',
								FORCE_MOW: 'Force the mower to mow for the specified amount of time. When the time has elapsed, the override is removed and the Planner reverts to the Calendar instead',
							},
							read: true,
							write: false,
						},
						native: {},
					});
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.planner.restrictedReason`, {
						type: 'state',
						common: {
							name: 'Restricted reason',
							type: 'string',
							role: 'state',
							states: {
								NONE: 'No restricted reason.',
								WEEK_SCHEDULE: 'There is no task in the Calendar right now, nothing to do.',
								PARK_OVERRIDE: 'The restriction is because someone forced us to park, using the override feature.',
								SENSOR: 'The sensor has decided that the grass is short enough, so there is no need to wear it down even more.',
								DAILY_LIMIT: 'If a model has a maximum allowed mowing time per day, this restriction will apply when that time has run out.',
								FOTA: 'When a Fota update is being transferred to the mower, we want to remain in the charging station to ensure that the transfer is successful. The restriction is removed when the transfer is done.',
								FROST: 'The frost sensor thinks it is too cold to mow.',
								ALL_WORK_AREAS_COMPLETED: 'All work areas are completed.',
								EXTERNAL: 'An external reason set by an external tool. Can be IFTTT, Google Assistant or Amazon Alexa. See externalReason for more information.',
							},
							read: true,
							write: false,
						},
						native: {},
					});
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.planner.externalReason`, {
						type: 'state',
						common: {
							name: 'External reason set by i.e. IFTTT, Google Assistant or Amazon Alexa.',
							type: 'number',
							role: 'state',
							min: 1000,
							max: 300000,
							read: true,
							write: false,
						},
						native: {},
					});

					// create channel "metadata"
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.metadata`, {
						type: 'channel',
						common: {
							name: 'Information if the mower is connected to the cloud and when last status was reported by the mower to the cloud.',
						},
						native: {},
					});
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.metadata.connected`, {
						type: 'state',
						common: {
							name: 'Is the mower currently connected to the cloud. The mower needs to be connected to send command to the mower.',
							type: 'boolean',
							role: 'indicator.connected',
							read: true,
							write: false,
						},
						native: {},
					});
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.metadata.statusTimestamp`, {
						type: 'state',
						common: {
							name: 'Timestamp for the last status update in milliseconds since 1970-01-01T00:00:00 in UTC time. NOTE! This timestamp is generated in the backend and not from the Mower.',
							type: 'number',
							role: 'value.time',
							read: true,
							write: false,
						},
						native: {},
					});

					// create channel GPS-"positions" if supported
					// if (mowerData.data[i].attributes.capabilities.position) {
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.positions`, {
						type: 'channel',
						common: {
							name: 'List of the GPS positions. Latest registered position is first in the array and the oldest last in the array. Max number of positions is 50 after that the latest position is removed from the array.',
						},
						native: {},
					});
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.positions.latitude`, {
						type: 'state',
						common: {
							name: 'Position latitude',
							type: 'number',
							role: 'value.gps.latitude',
							unit: '°',
							read: true,
							write: false,
						},
						native: {},
					});
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.positions.longitude`, {
						type: 'state',
						common: {
							name: 'Position longitude',
							type: 'number',
							role: 'value.gps.longitude',
							unit: '°',
							read: true,
							write: false,
						},
						native: {},
					});
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.positions.latlong`, {
						type: 'state',
						common: {
							name: 'Position "latitude;longitude"',
							type: 'string',
							role: 'value.gps',
							read: true,
							write: false,
						},
						native: {},
					});
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.positions.positions`, {
						type: 'state',
						common: {
							name: 'Positions',
							type: 'string',
							role: 'state',
							read: true,
							write: false,
						},
						native: {},
					});
					// }

					// create channel "statistics"
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.statistics`, {
						type: 'channel',
						common: {
							name: 'Information about the statistics. If a value is missing the mower does not support the value.',
						},
						native: {},
					});
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.statistics.cuttingBladeUsageTime`, {
						type: 'state',
						common: {
							name: 'The number of seconds since the last reset of the cutting blade usage counter.',
							type: 'number',
							role: 'state',
							unit: 's',
							read: true,
							write: false,
						},
						native: {},
					});
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.statistics.numberOfChargingCycles`, {
						type: 'state',
						common: {
							name: 'Numbers of charging cycles.',
							type: 'number',
							role: 'state',
							read: true,
							write: false,
						},
						native: {},
					});
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.statistics.numberOfCollisions`, {
						type: 'state',
						common: {
							name: 'The total number of collisions.',
							type: 'number',
							role: 'state',
							read: true,
							write: false,
						},
						native: {},
					});
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.statistics.totalChargingTime`, {
						type: 'state',
						common: {
							name: 'Total charging time in seconds.',
							type: 'number',
							role: 'state',
							unit: 's',
							read: true,
							write: false,
						},
						native: {},
					});
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.statistics.totalCuttingTime`, {
						type: 'state',
						common: {
							name: 'Total cutting time in seconds.',
							type: 'number',
							role: 'state',
							unit: 's',
							read: true,
							write: false,
						},
						native: {},
					});
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.statistics.totalDriveDistance`, {
						type: 'state',
						common: {
							name: "Total driven distance in meters. It's a calculated value based on totalRunningTime multiply with average speed for the mower depending on the model.",
							type: 'number',
							role: 'state',
							unit: 'm',
							read: true,
							write: false,
						},
						native: {},
					});
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.statistics.totalRunningTime`, {
						type: 'state',
						common: {
							name: 'The total running time in seconds (the wheel motors have been running).',
							type: 'number',
							role: 'state',
							unit: 's',
							read: true,
							write: false,
						},
						native: {},
					});
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.statistics.totalSearchingTime`, {
						type: 'state',
						common: {
							name: 'The total searching time in seconds.',
							type: 'number',
							role: 'state',
							unit: 's',
							read: true,
							write: false,
						},
						native: {},
					});

					// create channel "messages" (error/event history), see https://developer.husqvarnagroup.cloud/apis/Automower+Connect+API#/readme (GET .../messages)
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.messages`, {
						type: 'channel',
						common: {
							name: 'History of diagnostic and error messages reported by the Automower (latest first).',
						},
						native: {},
					});
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.messages.messages`, {
						type: 'state',
						common: {
							name: 'Full list of messages as returned by the API (JSON array, latest first, max. ~1000 entries).',
							type: 'string',
							role: 'json',
							read: true,
							write: false,
						},
						native: {},
					});
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.messages.lastTime`, {
						type: 'state',
						common: {
							name: 'Timestamp of the latest message.',
							type: 'number',
							role: 'value.time',
							read: true,
							write: false,
						},
						native: {},
					});
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.messages.lastCode`, {
						type: 'state',
						common: {
							name: 'Error/event code of the latest message (human readable).',
							type: 'string',
							role: 'state',
							read: true,
							write: false,
						},
						native: {},
					});
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.messages.lastSeverity`, {
						type: 'state',
						common: {
							name: 'Severity of the latest message (fatal, error, warning, info, debug, sw, unknown).',
							type: 'string',
							role: 'state',
							read: true,
							write: false,
						},
						native: {},
					});
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.messages.lastLatitude`, {
						type: 'state',
						common: {
							name: 'Latitude of the mower when the latest message was raised.',
							type: 'number',
							role: 'value.gps.latitude',
							read: true,
							write: false,
						},
						native: {},
					});
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.messages.lastLongitude`, {
						type: 'state',
						common: {
							name: 'Longitude of the mower when the latest message was raised.',
							type: 'number',
							role: 'value.gps.longitude',
							read: true,
							write: false,
						},
						native: {},
					});

					// create channel "STAYOUTZONES" if supported
					if (mowerData.data[i].attributes.capabilities.stayOutZones) {
						await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.stayOutZones`, {
							type: 'channel',
							common: {
								name: 'Information about stay-out zones if supported by the Automower. Stay-out zones are managed in the Automower Connect app. To create a stay-out zone you need to use the Automower Connect app. You can create stay-out zones around areas of your lawn that you do not want the mower to enter: for example, if you have an area with newly sown grass or beautiful spring flowers. The stay-out zone can be activated or deactivated, but not scheduled.',
							},
							native: {},
						});
						await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.stayOutZones.dirty`, {
							type: 'state',
							common: {
								name: 'If the stay-out zones are synchronized with the Husqvarna cloud. If the map is dirty you can not enable or disable a stay-out zone.',
								type: 'boolean',
								role: 'state',
								read: true,
								write: false,
							},
							native: {},
						});
						await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.stayOutZones.zones`, {
							type: 'state',
							common: {
								name: 'List of all stay-out zones for the Automower.',
								type: 'array',
								role: 'state',
								read: true,
								write: false,
							},
							native: {},
						});
					}

					// create channel "workAreas" if supported
					if (mowerData.data[i].attributes.capabilities.workAreas) {
						await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.workAreas`, {
							type: 'channel',
							common: {
								name: 'List of all work areas if supported by the Automower®. If empty list, no work areas are created. By default there should be a default work area with id 0.',
							},
							native: {},
						});

						for (let j = 0; j < mowerData.data[i].attributes.workAreas.length; j++) {
							// create channel "workAreaId"
							await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.workAreas.${mowerData.data[i].attributes.workAreas[j].workAreaId}`, {
								type: 'channel',
								common: {
									name: 'Work Area',
								},
								native: {},
							});

							await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.workAreas.${mowerData.data[i].attributes.workAreas[j].workAreaId}.workAreaId`, {
								type: 'state',
								common: {
									name: 'Work area ID',
									type: 'number',
									role: 'state',
									read: true,
									write: false,
								},
								native: {},
							});
							await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.workAreas.${mowerData.data[i].attributes.workAreas[j].workAreaId}.name`, {
								type: 'state',
								common: {
									name: 'Name of the work area',
									type: 'string',
									role: 'state',
									read: true,
									write: false,
								},
								native: {},
							});
							await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.workAreas.${mowerData.data[i].attributes.workAreas[j].workAreaId}.cuttingHeight`, {
								type: 'state',
								common: {
									name: 'Cutting height in percent (0 ... 100%)',
									type: 'number',
									role: 'state',
									min: 0,
									max: 100,
									read: true,
									write: false,
								},
								native: {},
							});
							await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.workAreas.${mowerData.data[i].attributes.workAreas[j].workAreaId}.enabled`, {
								type: 'state',
								common: {
									name: 'If the work area is enabled or disabled.',
									type: 'boolean',
									role: 'indicator.connected',
									read: true,
									write: false,
								},
								native: {},
							});
							await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.workAreas.${mowerData.data[i].attributes.workAreas[j].workAreaId}.progress`, {
								type: 'state',
								common: {
									name: 'The progrss on a work are. Only available for EPOS mowers and systematic mowing work areas.',
									type: 'number',
									role: 'state',
									min: 0,
									max: 100,
									read: true,
									write: false,
								},
								native: {},
							});
							await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.workAreas.${mowerData.data[i].attributes.workAreas[j].workAreaId}.lastTimeCompleted`, {
								type: 'state',
								common: {
									name: 'Timestamp in seconds from 1970-01-01 when the work area was last completed. The timestamp is in local time on the mower. Only available for EPOS mowers and systematic mowing work areas.',
									type: 'number',
									role: 'state',
									read: true,
									write: false,
								},
								native: {},
							});
							await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.workAreas.${mowerData.data[i].attributes.workAreas[j].workAreaId}.lastTimeAbandoned`, {
								type: 'state',
								common: {
									name: 'Timestamp in seconds from 1970-01-01 when the work area was last abandoned. Only available for EPOS mowers and systematic mowing work areas.',
									type: 'number',
									role: 'state',
									read: true,
									write: false,
								},
								native: {},
							});
							await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.workAreas.${mowerData.data[i].attributes.workAreas[j].workAreaId}.type`, {
								type: 'state',
								common: {
									name: 'Type of the work area (random or systematic mowing).',
									type: 'string',
									role: 'state',
									states: {
										random: 'Random mowing pattern (standard Automower behaviour).',
										systematic: 'Systematic mowing pattern (EPOS mowers).',
									},
									read: true,
									write: false,
								},
								native: {},
							});
							await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.workAreas.${mowerData.data[i].attributes.workAreas[j].workAreaId}.useGlobalCuttingHeight`, {
								type: 'state',
								common: {
									name: 'If true, the global cutting height setting is used instead of the cuttingHeight of this work area.',
									type: 'boolean',
									role: 'indicator',
									read: true,
									write: false,
								},
								native: {},
							});
							await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.workAreas.${mowerData.data[i].attributes.workAreas[j].workAreaId}.orientation`, {
								type: 'state',
								common: {
									name: 'Configured mowing orientation in degrees (0-1800, only for systematic mowing work areas).',
									type: 'number',
									role: 'value',
									min: 0,
									max: 1800,
									read: true,
									write: false,
								},
								native: {},
							});
							await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.workAreas.${mowerData.data[i].attributes.workAreas[j].workAreaId}.orientationShift`, {
								type: 'state',
								common: {
									name: 'Configured shift added between mowing sessions in degrees (0-1800, only for systematic mowing work areas).',
									type: 'number',
									role: 'value',
									min: 0,
									max: 1800,
									read: true,
									write: false,
								},
								native: {},
							});
							await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.workAreas.${mowerData.data[i].attributes.workAreas[j].workAreaId}.currentOrientation`, {
								type: 'state',
								common: {
									name: 'Current mowing orientation in degrees (only for systematic mowing work areas).',
									type: 'number',
									role: 'value',
									read: true,
									write: false,
								},
								native: {},
							});
							/*
							await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.workAreas.${mowerData.data[i].attributes.workAreas[j].workAreaId}.calendar`, {
								type: 'state',
								common: {
									name: 'Information about the calendar tasks. An Automower® can have several tasks. If the mower supports work areas the property workAreaId is required to connect the task to an work area.',
									type: 'string',
									role: 'state',
									read: true,
									write: false,
								},
								native: {},
							});
							*/
						}
					}

					// create channel "ACTIONS"
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.ACTIONS`, {
						type: 'channel',
						common: {
							name: 'Action Commands',
						},
						native: {},
					});
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.ACTIONS.PAUSE`, {
						type: 'state',
						common: {
							name: 'Pause the mower',
							type: 'boolean',
							def: false,
							role: 'button',
							read: true,
							write: true,
						},
						native: {},
					});
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.ACTIONS.PARKUNTILNEXTSCHEDULE`, {
						type: 'state',
						common: {
							name: 'Park the mower until next scheduled run.',
							type: 'boolean',
							def: false,
							role: 'button',
							read: true,
							write: true,
						},
						native: {},
					});
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.ACTIONS.PARKUNTILFURTHERNOTICE`, {
						type: 'state',
						common: {
							name: 'Parks the mower for ever. Needs to be manually started again.',
							type: 'boolean',
							def: false,
							role: 'button',
							read: true,
							write: true,
						},
						native: {},
					});

					// create channel "ACTIONS.park"
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.ACTIONS.park`, {
						type: 'channel',
						common: {
							name: 'Parks the mower for a period of minutes. The mower will drive to the charching station and park for the duration set by the commands.',
						},
						native: {},
					});
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.ACTIONS.park.PARK`, {
						type: 'state',
						common: {
							name: 'Parks the mower for a period of minutes. The mower will drive to the charching station and park for the duration set by the commands.',
							type: 'boolean',
							def: false,
							role: 'button',
							read: true,
							write: true,
						},
						native: {},
					});
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.ACTIONS.park.parkTime`, {
						type: 'state',
						common: {
							name: 'Parks the mower for a period of minutes: Time',
							type: 'number',
							def: 15,
							role: 'state',
							unit: 'min',
							min: 1,
							read: true,
							write: true,
						},
						native: {},
					});
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.ACTIONS.RESUMESCHEDULE`, {
						type: 'state',
						common: {
							name: 'Removes any ovveride on the Planner and let the mower resume to the schedule set by the Calendar.',
							type: 'boolean',
							def: false,
							role: 'button',
							read: true,
							write: true,
						},
						native: {},
					});

					// create channel "ACTIONS.start"
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.ACTIONS.start`, {
						type: 'channel',
						common: {
							name: 'Start the mower for a period of minutes.',
						},
						native: {},
					});
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.ACTIONS.start.START`, {
						type: 'state',
						common: {
							name: 'Start the mower for a period of minutes.',
							type: 'boolean',
							def: false,
							role: 'button',
							read: true,
							write: true,
						},
						native: {},
					});
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.ACTIONS.start.startTime`, {
						type: 'state',
						common: {
							name: 'Start the mower for a period of minutes: Time',
							type: 'number',
							def: 15,
							role: 'state',
							unit: 'min',
							min: 1,
							read: true,
							write: true,
						},
						native: {},
					});

					// create channel "ACTIONS.startInWorkArea" if supported
					if (mowerData.data[i].attributes.capabilities.workAreas) {
						await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.ACTIONS.startInWorkArea`, {
							type: 'channel',
							common: {
								name: 'Start the mower in a work area for a period of minutes. If duration is skipped the mower will continue forever.',
							},
							native: {},
						});
						await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.ACTIONS.startInWorkArea.startTime`, {
							type: 'state',
							common: {
								name: 'Optional. Duration period in minutes, if zero (0) the override will be forever',
								type: 'number',
								def: 0,
								role: 'state',
								unit: 'min',
								min: 0,
								read: true,
								write: true,
							},
							native: {},
						});
						await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.ACTIONS.startInWorkArea.workAreaId`, {
							type: 'state',
							common: {
								name: 'Id on the work area',
								type: 'number',
								def: 0,
								role: 'state',
								min: 0,
								read: true,
								write: true,
							},
							native: {},
						});
						await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.ACTIONS.startInWorkArea.STARTINWORKAREA`, {
							type: 'state',
							common: {
								name: 'Start the mower in a work area for a period of minutes. If duration is skipped the mower will continue forever',
								type: 'boolean',
								def: false,
								role: 'button',
								read: true,
								write: true,
							},
							native: {},
						});

						// create channel "ACTIONS.workAreaSettings" (update cutting height / enabled state of a single work area)
						await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.ACTIONS.workAreaSettings`, {
							type: 'channel',
							common: {
								name: 'Update the cutting height and/or enabled state of a single work area. Set workAreaId, cuttingHeight and/or enabled, then trigger APPLYWORKAREASETTINGS.',
							},
							native: {},
						});
						await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.ACTIONS.workAreaSettings.workAreaId`, {
							type: 'state',
							common: {
								name: 'ID of the work area to update.',
								type: 'number',
								role: 'state',
								def: 0,
								read: true,
								write: true,
							},
							native: {},
						});
						await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.ACTIONS.workAreaSettings.cuttingHeight`, {
							type: 'state',
							common: {
								name: 'Cutting height in percent (0 ... 100%) to set for the selected work area.',
								type: 'number',
								role: 'level',
								min: 0,
								max: 100,
								def: 50,
								read: true,
								write: true,
							},
							native: {},
						});
						await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.ACTIONS.workAreaSettings.enabled`, {
							type: 'state',
							common: {
								name: 'Enabled state to set for the selected work area.',
								type: 'boolean',
								role: 'switch.enable',
								def: true,
								read: true,
								write: true,
							},
							native: {},
						});
						await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.ACTIONS.workAreaSettings.APPLYWORKAREASETTINGS`, {
							type: 'state',
							common: {
								name: 'Apply cuttingHeight and/or enabled to the work area given by workAreaId.',
								type: 'boolean',
								def: false,
								role: 'button',
								read: true,
								write: true,
							},
							native: {},
						});
					}

					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.ACTIONS.CUTTINGHEIGHT`, {
						type: 'state',
						common: {
							name: 'Prescaled cutting height, Range: 1 to 9',
							type: 'number',
							role: 'state',
							min: 1,
							max: 9,
							read: true,
							write: true,
						},
						native: {},
					});
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.ACTIONS.DATETIME`, {
						type: 'state',
						common: {
							name: 'Date and time in seconds from 1970-01-01 in the mower. The timestamp is used by the mower to trigger the schedule',
							type: 'number',
							role: 'state',
							min: 1725141600,
							read: true,
							write: true,
						},
						native: {},
					});

					// create DP "ACTIONS.HEADLIGHT" if supported
					if (mowerData.data[i].attributes.capabilities.headlights) {
						await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.ACTIONS.HEADLIGHT`, {
							type: 'state',
							common: {
								name: 'Information about headlights. Not all Automower models supports headlight and not all enums are available for all models.',
								type: 'string',
								role: 'state',
								states: {
									ALWAYS_ON: 'Always on.',
									ALWAYS_OFF: 'Always off.',
									EVENING_ONLY: 'Only in the evening.',
									EVENING_AND_NIGHT: 'In evening and night.',
								},
								read: true,
								write: true,
							},
							native: {},
						});
					}

					// create channel "ACTIONS.stayOutZoneSettings" (enable/disable a single stay-out zone) if supported
					if (mowerData.data[i].attributes.capabilities.stayOutZones) {
						await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.ACTIONS.stayOutZoneSettings`, {
							type: 'channel',
							common: {
								name: 'Enable or disable a single stay-out zone. Set zoneId and enabled, then trigger APPLYSTAYOUTZONESETTINGS. Not possible while stayOutZones.dirty is true.',
							},
							native: {},
						});
						await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.ACTIONS.stayOutZoneSettings.zoneId`, {
							type: 'state',
							common: {
								name: 'ID (UUID) of the stay-out zone to update, e.g. taken from stayOutZones.zones.',
								type: 'string',
								role: 'state',
								def: '',
								read: true,
								write: true,
							},
							native: {},
						});
						await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.ACTIONS.stayOutZoneSettings.enabled`, {
							type: 'state',
							common: {
								name: 'Enabled state to set for the selected stay-out zone.',
								type: 'boolean',
								role: 'switch.enable',
								def: true,
								read: true,
								write: true,
							},
							native: {},
						});
						await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.ACTIONS.stayOutZoneSettings.APPLYSTAYOUTZONESETTINGS`, {
							type: 'state',
							common: {
								name: 'Apply enabled to the stay-out zone given by zoneId.',
								type: 'boolean',
								def: false,
								role: 'button',
								read: true,
								write: true,
							},
							native: {},
						});
					}

					// create channel "ACTIONS.schedule"
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.ACTIONS.schedule`, {
						type: 'channel',
						common: {
							name: 'Update mower schedule',
						},
						native: {},
					});
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.ACTIONS.schedule.SET`, {
						type: 'state',
						common: {
							name: 'Save all schedules',
							type: 'boolean',
							role: 'button',
							def: false,
							read: true,
							write: true,
						},
						native: {},
					});

					this.numberOfSchedules = mowerData.data[i].attributes.calendar.tasks.length;
					for (let j = 0; j < mowerData.data[i].attributes.calendar.tasks.length; j++) {
						await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.ACTIONS.schedule.${j}`, {
							type: 'channel',
							common: {
								name: `Scheduled Task ${j}`,
							},
							native: {},
						});
						await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.ACTIONS.schedule.${j}.start`, {
							type: 'state',
							common: {
								name: 'Start time expressed in minutes after midnight',
								type: 'number',
								role: 'level',
								min: 0,
								max: 1439,
								unit: 'min',
								def: 720,
								read: true,
								write: true,
							},
							native: {},
						});
						await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.ACTIONS.schedule.${j}.duration`, {
							type: 'state',
							common: {
								name: 'Duration time expressed in minutes',
								type: 'number',
								role: 'level',
								min: 1,
								max: 1440,
								unit: 'min',
								def: 30,
								read: true,
								write: true,
							},
							native: {},
						});
						await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.ACTIONS.schedule.${j}.monday`, {
							type: 'state',
							common: {
								name: 'Enabled on Mondays',
								type: 'boolean',
								role: 'switch',
								def: false,
								read: true,
								write: true,
							},
							native: {},
						});
						await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.ACTIONS.schedule.${j}.tuesday`, {
							type: 'state',
							common: {
								name: 'Enabled on Tuesdays',
								type: 'boolean',
								role: 'switch',
								def: false,
								read: true,
								write: true,
							},
							native: {},
						});
						await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.ACTIONS.schedule.${j}.wednesday`, {
							type: 'state',
							common: {
								name: 'Enabled on Wednesdays',
								type: 'boolean',
								role: 'switch',
								def: false,
								read: true,
								write: true,
							},
							native: {},
						});
						await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.ACTIONS.schedule.${j}.thursday`, {
							type: 'state',
							common: {
								name: 'Enabled on Thursdays',
								type: 'boolean',
								role: 'switch',
								def: false,
								read: true,
								write: true,
							},
							native: {},
						});
						await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.ACTIONS.schedule.${j}.friday`, {
							type: 'state',
							common: {
								name: 'Enabled on Fridays',
								type: 'boolean',
								role: 'switch',
								def: false,
								read: true,
								write: true,
							},
							native: {},
						});
						await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.ACTIONS.schedule.${j}.saturday`, {
							type: 'state',
							common: {
								name: 'Enabled on Saturdays',
								type: 'boolean',
								role: 'switch',
								def: false,
								read: true,
								write: true,
							},
							native: {},
						});
						await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.ACTIONS.schedule.${j}.sunday`, {
							type: 'state',
							common: {
								name: 'Enabled on Sundays',
								type: 'boolean',
								role: 'switch',
								def: false,
								read: true,
								write: true,
							},
							native: {},
						});
						// create state "workAreaId" if supported
						if (mowerData.data[i].attributes.capabilities.workAreas) {
							await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.ACTIONS.schedule.${j}.workAreaId`, {
								type: 'state',
								common: {
									name: 'Workarea ID',
									type: 'number',
									role: 'state',
									read: true,
									write: true,
								},
								native: {},
							});
						}
					}
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.ACTIONS.REFRESHSTATISTICS`, {
						type: 'state',
						common: {
							name: 'Refresh statistic values outside the regular configured schedule',
							type: 'boolean',
							def: false,
							role: 'button',
							read: true,
							write: true,
						},
						native: {},
					});
					await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.ACTIONS.RESETCUTTINGBLADEUSAGETIME`, {
						type: 'state',
						common: {
							name: 'Reset the cutting blade usage time counter (statistics.cuttingBladeUsageTime). Same function as in the Automower Connect app, use after changing the blades.',
							type: 'boolean',
							def: false,
							role: 'button',
							read: true,
							write: true,
						},
						native: {},
					});
					// create DP "ACTIONS.CONFIRMERROR" if supported
					if (mowerData.data[i].attributes.capabilities.canConfirmError) {
						await this.setObjectNotExistsAsync(`${mowerData.data[i].id}.ACTIONS.CONFIRMERROR`, {
							type: 'state',
							common: {
								name: 'Confirm a non-fatal mower error (only works if mower.isErrorConfirmable is true).',
								type: 'boolean',
								def: false,
								role: 'button',
								read: true,
								write: true,
							},
							native: {},
						});
					}

					// subscribeStates
					this.subscribeStates(`${mowerData.data[i].id}.ACTIONS.PAUSE`);
					this.subscribeStates(`${mowerData.data[i].id}.ACTIONS.PARKUNTILNEXTSCHEDULE`);
					this.subscribeStates(`${mowerData.data[i].id}.ACTIONS.PARKUNTILFURTHERNOTICE`);
					this.subscribeStates(`${mowerData.data[i].id}.ACTIONS.park.PARK`);
					this.subscribeStates(`${mowerData.data[i].id}.ACTIONS.RESUMESCHEDULE`);
					this.subscribeStates(`${mowerData.data[i].id}.ACTIONS.start.START`);
					if (mowerData.data[i].attributes.capabilities.workAreas) {
						this.subscribeStates(`${mowerData.data[i].id}.ACTIONS.startInWorkArea.STARTINWORKAREA`);
						this.subscribeStates(`${mowerData.data[i].id}.ACTIONS.workAreaSettings.APPLYWORKAREASETTINGS`);
					}
					this.subscribeStates(`${mowerData.data[i].id}.ACTIONS.CUTTINGHEIGHT`);
					this.subscribeStates(`${mowerData.data[i].id}.ACTIONS.DATETIME`);
					if (mowerData.data[i].attributes.capabilities.headlights) {
						this.subscribeStates(`${mowerData.data[i].id}.ACTIONS.HEADLIGHT`);
					}
					if (mowerData.data[i].attributes.capabilities.stayOutZones) {
						this.subscribeStates(`${mowerData.data[i].id}.ACTIONS.stayOutZoneSettings.APPLYSTAYOUTZONESETTINGS`);
					}
					this.subscribeStates(`${mowerData.data[i].id}.ACTIONS.schedule.SET`);
					this.subscribeStates(`${mowerData.data[i].id}.ACTIONS.REFRESHSTATISTICS`);
					this.subscribeStates(`${mowerData.data[i].id}.ACTIONS.RESETCUTTINGBLADEUSAGETIME`);
					if (mowerData.data[i].attributes.capabilities.canConfirmError) {
						this.subscribeStates(`${mowerData.data[i].id}.ACTIONS.CONFIRMERROR`);
					}
				} else {
					throw new Error('No mower found, no Objects created. Check API (ERR_#007).');
				}
			}
			this.log.debug('[createObjects]: Objects created...');
		} else {
			throw new Error('No Objects found, no Objects created. Check API (ERR_#008).');
		}
	}

	async fillObjects(mowerData) {
		for (const i in mowerData.data) {
			if ('attributes' in mowerData.data[i]) {
				if (this.firstStart) {
					this.setState(`${mowerData.data[i].id}.system.type`, {
						val: mowerData.data[i].type,
						ack: true,
					});
					this.setState(`${mowerData.data[i].id}.system.id`, {
						val: mowerData.data[i].id,
						ack: true,
					});
					this.setState(`${mowerData.data[i].id}.system.name`, {
						val: mowerData.data[i].attributes.system.name,
						ack: true,
					});
					this.setState(`${mowerData.data[i].id}.system.model`, {
						val: mowerData.data[i].attributes.system.model,
						ack: true,
					});
					this.setState(`${mowerData.data[i].id}.system.serialNumber`, {
						val: mowerData.data[i].attributes.system.serialNumber,
						ack: true,
					});

					this.setState(`${mowerData.data[i].id}.battery.batteryPercent`, {
						val: mowerData.data[i].attributes.battery.batteryPercent,
						ack: true,
					});

					this.setState(`${mowerData.data[i].id}.capabilities.canConfirmError`, {
						val: mowerData.data[i].attributes.capabilities.canConfirmError,
						ack: true,
					});
					this.setState(`${mowerData.data[i].id}.capabilities.headlights`, {
						val: mowerData.data[i].attributes.capabilities.headlights,
						ack: true,
					});
					this.setState(`${mowerData.data[i].id}.capabilities.position`, {
						val: mowerData.data[i].attributes.capabilities.position,
						ack: true,
					});
					this.setState(`${mowerData.data[i].id}.capabilities.stayOutZones`, {
						val: mowerData.data[i].attributes.capabilities.stayOutZones,
						ack: true,
					});
					this.setState(`${mowerData.data[i].id}.capabilities.workAreas`, {
						val: mowerData.data[i].attributes.capabilities.workAreas,
						ack: true,
					});

					this.setState(`${mowerData.data[i].id}.mower.mode`, {
						val: mowerData.data[i].attributes.mower.mode,
						ack: true,
					});
					this.setState(`${mowerData.data[i].id}.mower.activity`, {
						val: mowerData.data[i].attributes.mower.activity,
						ack: true,
					});
					this.setState(`${mowerData.data[i].id}.mower.inactiveReason`, {
						val: mowerData.data[i].attributes.mower.inactiveReason,
						ack: true,
					});
					this.setState(`${mowerData.data[i].id}.mower.state`, {
						val: mowerData.data[i].attributes.mower.state,
						ack: true,
					});
					this.setState(`${mowerData.data[i].id}.mower.workAreaId`, {
						val: mowerData.data[i].attributes.mower.workAreaId,
						ack: true,
					});
					this.setState(`${mowerData.data[i].id}.mower.errorCode`, {
						val: mowerData.data[i].attributes.mower.errorCode,
						ack: true,
					});
					this.setState(`${mowerData.data[i].id}.mower.errorCodeTimestamp`, {
						val: mowerData.data[i].attributes.mower.errorCodeTimestamp,
						ack: true,
					});
					this.setState(`${mowerData.data[i].id}.mower.isErrorConfirmable`, {
						val: mowerData.data[i].attributes.mower.isErrorConfirmable,
						ack: true,
					});

					// set all values in "calendar"
					for (let j = 0; j < Object.keys(mowerData.data[i].attributes.calendar.tasks).length; j++) {
						this.setState(`${mowerData.data[i].id}.ACTIONS.schedule.${[j]}.start`, {
							val: mowerData.data[i].attributes.calendar.tasks[j].start,
							ack: true,
						});
						this.setState(`${mowerData.data[i].id}.ACTIONS.schedule.${[j]}.duration`, {
							val: mowerData.data[i].attributes.calendar.tasks[j].duration,
							ack: true,
						});
						this.setState(`${mowerData.data[i].id}.ACTIONS.schedule.${[j]}.monday`, {
							val: mowerData.data[i].attributes.calendar.tasks[j].monday,
							ack: true,
						});
						this.setState(`${mowerData.data[i].id}.ACTIONS.schedule.${[j]}.tuesday`, {
							val: mowerData.data[i].attributes.calendar.tasks[j].tuesday,
							ack: true,
						});
						this.setState(`${mowerData.data[i].id}.ACTIONS.schedule.${[j]}.wednesday`, {
							val: mowerData.data[i].attributes.calendar.tasks[j].wednesday,
							ack: true,
						});
						this.setState(`${mowerData.data[i].id}.ACTIONS.schedule.${[j]}.thursday`, {
							val: mowerData.data[i].attributes.calendar.tasks[j].thursday,
							ack: true,
						});
						this.setState(`${mowerData.data[i].id}.ACTIONS.schedule.${[j]}.friday`, {
							val: mowerData.data[i].attributes.calendar.tasks[j].friday,
							ack: true,
						});
						this.setState(`${mowerData.data[i].id}.ACTIONS.schedule.${[j]}.saturday`, {
							val: mowerData.data[i].attributes.calendar.tasks[j].saturday,
							ack: true,
						});
						this.setState(`${mowerData.data[i].id}.ACTIONS.schedule.${[j]}.sunday`, {
							val: mowerData.data[i].attributes.calendar.tasks[j].sunday,
							ack: true,
						});
						if (mowerData.data[i].attributes.capabilities.workAreas) {
							this.setState(`${mowerData.data[i].id}.ACTIONS.schedule.${[j]}.workAreaId`, {
								val: mowerData.data[i].attributes.calendar.tasks[j].workAreaId,
								ack: true,
							});
						}
					}

					this.setState(`${mowerData.data[i].id}.planner.nextStartTimestamp`, {
						val: mowerData.data[i].attributes.planner.nextStartTimestamp,
						ack: true,
					});
					this.setState(`${mowerData.data[i].id}.planner.override`, {
						val: mowerData.data[i].attributes.planner.override.action,
						ack: true,
					});
					this.setState(`${mowerData.data[i].id}.planner.restrictedReason`, {
						val: mowerData.data[i].attributes.planner.restrictedReason,
						ack: true,
					});
					if (mowerData.data[i].attributes.capabilities.position && mowerData.data[i].attributes.positions.length > 0) {
						this.setState(`${mowerData.data[i].id}.positions.latitude`, {
							val: mowerData.data[i].attributes.positions[0].latitude,
							ack: true,
						});
						this.setState(`${mowerData.data[i].id}.positions.longitude`, {
							val: mowerData.data[i].attributes.positions[0].longitude,
							ack: true,
						});
						this.setState(`${mowerData.data[i].id}.positions.latlong`, {
							val: `${mowerData.data[i].attributes.positions[0].latitude};${mowerData.data[i].attributes.positions[0].longitude}`,
							ack: true,
						});
						this.setState(`${mowerData.data[i].id}.positions.positions`, {
							val: `${JSON.stringify(mowerData.data[i].attributes.positions)}`,
							ack: true,
						});
					}

					this.setState(`${mowerData.data[i].id}.ACTIONS.CUTTINGHEIGHT`, {
						// NOTE: in the REST response "settings.cuttingHeight" is a plain number, not an object with ".height"
						// (that nested shape is only used inside the WebSocket "cuttingHeight" push event, see connectToWS()).
						val: mowerData.data[i].attributes.settings.cuttingHeight,
						ack: true,
					});
					if (mowerData.data[i].attributes.capabilities.headlights) {
						this.setState(`${mowerData.data[i].id}.ACTIONS.HEADLIGHT`, {
							val: mowerData.data[i].attributes.settings.headlight.mode,
							ack: true,
						});
					}
				}

				this.setState(`${mowerData.data[i].id}.metadata.connected`, {
					val: mowerData.data[i].attributes.metadata.connected,
					ack: true,
				});
				this.setState(`${mowerData.data[i].id}.metadata.statusTimestamp`, {
					val: mowerData.data[i].attributes.metadata.statusTimestamp,
					ack: true,
				});

				this.setState(`${mowerData.data[i].id}.statistics.cuttingBladeUsageTime`, {
					val: mowerData.data[i].attributes.statistics.cuttingBladeUsageTime,
					ack: true,
				});
				this.setState(`${mowerData.data[i].id}.statistics.numberOfChargingCycles`, {
					val: mowerData.data[i].attributes.statistics.numberOfChargingCycles,
					ack: true,
				});
				this.setState(`${mowerData.data[i].id}.statistics.numberOfCollisions`, {
					val: mowerData.data[i].attributes.statistics.numberOfCollisions,
					ack: true,
				});
				this.setState(`${mowerData.data[i].id}.statistics.totalChargingTime`, {
					val: mowerData.data[i].attributes.statistics.totalChargingTime,
					ack: true,
				});
				this.setState(`${mowerData.data[i].id}.statistics.totalCuttingTime`, {
					val: mowerData.data[i].attributes.statistics.totalCuttingTime,
					ack: true,
				});
				this.setState(`${mowerData.data[i].id}.statistics.totalDriveDistance`, {
					val: mowerData.data[i].attributes.statistics.totalDriveDistance,
					ack: true,
				});
				this.setState(`${mowerData.data[i].id}.statistics.totalRunningTime`, {
					val: mowerData.data[i].attributes.statistics.totalRunningTime,
					ack: true,
				});
				this.setState(`${mowerData.data[i].id}.statistics.totalSearchingTime`, {
					val: mowerData.data[i].attributes.statistics.totalSearchingTime,
					ack: true,
				});
				if (mowerData.data[i].attributes.capabilities.stayOutZones) {
					if (mowerData.data[i].attributes.stayOutZones) {
						if (mowerData.data[i].attributes.stayOutZones.dirty) {
							this.setState(`${mowerData.data[i].id}.stayOutZones.dirty`, {
								val: mowerData.data[i].attributes.stayOutZones.dirty,
								ack: true,
							});
						}
					}
					if (mowerData.data[i].attributes.stayOutZones) {
						if (mowerData.data[i].attributes.stayOutZones.zones) {
							this.setState(`${mowerData.data[i].id}.stayOutZones.zones`, {
								val: mowerData.data[i].attributes.stayOutZones.zones,
								ack: true,
							});
						}
					}
				}
				if (mowerData.data[i].attributes.capabilities.workAreas) {
					for (let j = 0; j < mowerData.data[i].attributes.workAreas.length; j++) {
						this.setState(`${mowerData.data[i].id}.workAreas.${mowerData.data[i].attributes.workAreas[j].workAreaId}.workAreaId`, {
							val: mowerData.data[i].attributes.workAreas[j].workAreaId,
							ack: true,
						});
						this.setState(`${mowerData.data[i].id}.workAreas.${mowerData.data[i].attributes.workAreas[j].workAreaId}.name`, {
							val: mowerData.data[i].attributes.workAreas[j].name,
							ack: true,
						});
						this.setState(`${mowerData.data[i].id}.workAreas.${mowerData.data[i].attributes.workAreas[j].workAreaId}.cuttingHeight`, {
							val: mowerData.data[i].attributes.workAreas[j].cuttingHeight,
							ack: true,
						});
						this.setState(`${mowerData.data[i].id}.workAreas.${mowerData.data[i].attributes.workAreas[j].workAreaId}.enabled`, {
							val: mowerData.data[i].attributes.workAreas[j].enabled,
							ack: true,
						});
						this.setState(`${mowerData.data[i].id}.workAreas.${mowerData.data[i].attributes.workAreas[j].workAreaId}.lastTimeCompleted`, {
							val: mowerData.data[i].attributes.workAreas[j].lastTimeCompleted,
							ack: true,
						});
						this.setState(`${mowerData.data[i].id}.workAreas.${mowerData.data[i].attributes.workAreas[j].workAreaId}.progress`, {
							val: mowerData.data[i].attributes.workAreas[j].progress,
							ack: true,
						});
						this.setState(`${mowerData.data[i].id}.workAreas.${mowerData.data[i].attributes.workAreas[j].workAreaId}.lastTimeAbandoned`, {
							val: mowerData.data[i].attributes.workAreas[j].lastTimeAbandoned,
							ack: true,
						});
						this.setState(`${mowerData.data[i].id}.workAreas.${mowerData.data[i].attributes.workAreas[j].workAreaId}.type`, {
							val: mowerData.data[i].attributes.workAreas[j].type,
							ack: true,
						});
						this.setState(`${mowerData.data[i].id}.workAreas.${mowerData.data[i].attributes.workAreas[j].workAreaId}.useGlobalCuttingHeight`, {
							val: mowerData.data[i].attributes.workAreas[j].useGlobalCuttingHeight,
							ack: true,
						});
						this.setState(`${mowerData.data[i].id}.workAreas.${mowerData.data[i].attributes.workAreas[j].workAreaId}.orientation`, {
							val: mowerData.data[i].attributes.workAreas[j].orientation,
							ack: true,
						});
						this.setState(`${mowerData.data[i].id}.workAreas.${mowerData.data[i].attributes.workAreas[j].workAreaId}.orientationShift`, {
							val: mowerData.data[i].attributes.workAreas[j].orientationShift,
							ack: true,
						});
						this.setState(`${mowerData.data[i].id}.workAreas.${mowerData.data[i].attributes.workAreas[j].workAreaId}.currentOrientation`, {
							val: mowerData.data[i].attributes.workAreas[j].currentOrientation,
							ack: true,
						});
						/*
						if (mowerData.data[i].attributes.workAreas[j].calendar) {
							this.setState(`${mowerData.data[i].id}.workAreas.${mowerData.data[i].attributes.workAreas[j].workAreaId}.calendar`, {
								val: mowerData.data[i].attributes.workAreas[j].calendar,
								ack: true,
							});
						}
						*/
					}
				}
				// update-or-insert instead of push: fillObjects() runs on every statistics poll (default every few minutes),
				// unconditional push() would otherwise leak memory by appending a duplicate entry on every cycle
				const capabilitiesEntry = {
					id: mowerData.data[i].id,
					workAreas: mowerData.data[i].attributes.capabilities.workAreas,
					numbersOfCalendars: Object.keys(mowerData.data[i].attributes.calendar.tasks).length,
				};
				const capabilitiesIndex = this.capabilities.findIndex(o => o.id === mowerData.data[i].id);
				if (capabilitiesIndex === -1) {
					this.capabilities.push(capabilitiesEntry);
				} else {
					this.capabilities[capabilitiesIndex] = capabilitiesEntry;
				}
			} else {
				this.log.error('[fillObjects]: No values found. Nothing updated (ERR_#009)');
			}
		}
		if (this.firstStart) {
			// this.log.info(`this.capabilities: ${JSON.stringify(this.capabilities)}`);
			this.log.info('Mowerdata initially saved.');
		} else {
			this.log.debug('Mowerstatistics updated.');
		}
	}

	// https://javascript.info/websocket
	// https://developer.husqvarnagroup.cloud/apis/automower-connect-api#websocket
	async connectToWS() {
		if (this.wss) {
			this.wss.close(1000, 'Close old websocket connection before start new websocket connection.');
		}

		this.wss = new WebSocket('wss://ws.openapi.husqvarna.dev/v1', {
			headers: {
				Authorization: `Bearer ${this.access_token}`,
			},
		});

		this.wss.on('open', () => {
			// connection succeeded - reset the reconnect backoff so a future failure starts from the base delay again
			this.wsReconnectDelay = 5000;

			if (this.firstStart === true) {
				this.log.info('Connection to "Husqvarna WebSocket" established. Ready to get data...');
				this.firstStart = false;
			} else {
				this.log.debug('[wss.on - open]: Connection to "Husqvarna WebSocket" re-established. Ready to get data...');
			}

			this.setState('info.connection', true, true);

			// Send ping to server
			this.sendPingToServer();
		});

		this.wss.on('message', async (data, isBinary) => {
			const message = isBinary ? JSON.parse(data) : JSON.parse(data.toString());
			this.log.debug(`[wss.on - message]: ${JSON.stringify(message)}`);

			try {
				if ('attributes' in message) {
					if ('cuttingHeight' in message.attributes) {
						this.setState(`${message.id}.ACTIONS.CUTTINGHEIGHT`, {
							val: message.attributes.cuttingHeight.height,
							ack: true,
						});
						// this.log.debug(`[wss.on - message]: message.attributes.cuttingHeight: ${message.attributes.cuttingHeight}`);
					}

					if ('headlight' in message.attributes) {
						this.setState(`${message.id}.ACTIONS.HEADLIGHT`, {
							val: message.attributes.headlight.mode,
							ack: true,
						});
						// this.log.debug(`[wss.on - message]: message.attributes.headlight.mode: ${message.attributes.headlight.mode}`);
					}

					// message-event-v2: a single new diagnostic/error message was pushed live; update the "last..." convenience
					// states immediately. The full history list is refreshed via getAndFillMowerMessages() (REST poll).
					if ('message' in message.attributes) {
						this.setState(`${message.id}.messages.lastTime`, {
							val: message.attributes.message.time,
							ack: true,
						});
						this.setState(`${message.id}.messages.lastCode`, {
							val: message.attributes.message.code,
							ack: true,
						});
						this.setState(`${message.id}.messages.lastSeverity`, {
							val: message.attributes.message.severity,
							ack: true,
						});
						this.setState(`${message.id}.messages.lastLatitude`, {
							val: message.attributes.message.latitude,
							ack: true,
						});
						this.setState(`${message.id}.messages.lastLongitude`, {
							val: message.attributes.message.longitude,
							ack: true,
						});
						// this.log.debug(`[wss.on - message]: message.attributes.message: ${JSON.stringify(message.attributes.message)}`);
					}

					if ('calendar' in message.attributes && Object.keys(message.attributes.calendar.tasks).length !== 0) {
						if (Object.keys(message.attributes.calendar.tasks).length !== this.capabilities.find(o => o.id === message.id).numbersOfCalendars) {
							// set values in "calendar"
							this.log.debug(`numbers of calendars changed`);

							// delete all existing entries
							for (let j = 0; j < this.capabilities.find(o => o.id === message.id).numbersOfCalendars; j++) {
								await this.delObjectAsync(`${message.id}.ACTIONS.schedule.${j}`, { recursive: true });
								this.log.debug(`${message.id}.ACTIONS.schedule.${j}`);
							}

							for (let k = 0; k < Object.keys(message.attributes.calendar.tasks).length; k++) {
								await this.setObjectNotExistsAsync(`${message.id}.ACTIONS.schedule.${k}`, {
									type: 'channel',
									common: {
										name: `Scheduled Task ${k}`,
									},
									native: {},
								});
								await this.setObjectNotExistsAsync(`${message.id}.ACTIONS.schedule.${k}.start`, {
									type: 'state',
									common: {
										name: 'Start time expressed in minutes after midnight',
										type: 'number',
										role: 'level',
										min: 0,
										max: 1439,
										unit: 'min',
										def: 720,
										read: true,
										write: true,
									},
									native: {},
								});
								await this.setObjectNotExistsAsync(`${message.id}.ACTIONS.schedule.${k}.duration`, {
									type: 'state',
									common: {
										name: 'Duration time expressed in minutes',
										type: 'number',
										role: 'level',
										min: 1,
										max: 1440,
										unit: 'min',
										def: 30,
										read: true,
										write: true,
									},
									native: {},
								});
								await this.setObjectNotExistsAsync(`${message.id}.ACTIONS.schedule.${k}.monday`, {
									type: 'state',
									common: {
										name: 'Enabled on Mondays',
										type: 'boolean',
										role: 'switch',
										def: false,
										read: true,
										write: true,
									},
									native: {},
								});
								await this.setObjectNotExistsAsync(`${message.id}.ACTIONS.schedule.${k}.tuesday`, {
									type: 'state',
									common: {
										name: 'Enabled on Tuesdays',
										type: 'boolean',
										role: 'switch',
										def: false,
										read: true,
										write: true,
									},
									native: {},
								});
								await this.setObjectNotExistsAsync(`${message.id}.ACTIONS.schedule.${k}.wednesday`, {
									type: 'state',
									common: {
										name: 'Enabled on Wednesdays',
										type: 'boolean',
										role: 'switch',
										def: false,
										read: true,
										write: true,
									},
									native: {},
								});
								await this.setObjectNotExistsAsync(`${message.id}.ACTIONS.schedule.${k}.thursday`, {
									type: 'state',
									common: {
										name: 'Enabled on Thursdays',
										type: 'boolean',
										role: 'switch',
										def: false,
										read: true,
										write: true,
									},
									native: {},
								});
								await this.setObjectNotExistsAsync(`${message.id}.ACTIONS.schedule.${k}.friday`, {
									type: 'state',
									common: {
										name: 'Enabled on Fridays',
										type: 'boolean',
										role: 'switch',
										def: false,
										read: true,
										write: true,
									},
									native: {},
								});
								await this.setObjectNotExistsAsync(`${message.id}.ACTIONS.schedule.${k}.saturday`, {
									type: 'state',
									common: {
										name: 'Enabled on Saturdays',
										type: 'boolean',
										role: 'switch',
										def: false,
										read: true,
										write: true,
									},
									native: {},
								});
								await this.setObjectNotExistsAsync(`${message.id}.ACTIONS.schedule.${k}.sunday`, {
									type: 'state',
									common: {
										name: 'Enabled on Sundays',
										type: 'boolean',
										role: 'switch',
										def: false,
										read: true,
										write: true,
									},
									native: {},
								});
								// create state "workAreaId" if supported
								if (message.attributes.calendar.workAreas) {
									await this.setObjectNotExistsAsync(`${message.id}.ACTIONS.schedule.${k}.workAreaId`, {
										type: 'state',
										common: {
											name: 'Workarea ID',
											type: 'number',
											role: 'state',
											read: true,
											write: true,
										},
										native: {},
									});
								}
							}
							this.capabilities[this.capabilities.findIndex(obj => obj.id == message.id)].numbersOfCalendars = Object.keys(message.attributes.calendar.tasks).length;
						}

						for (let i = 0; i < Object.keys(message.attributes.calendar.tasks).length; i++) {
							await this.setState(`${message.id}.ACTIONS.schedule.${[i]}.start`, {
								val: message.attributes.calendar.tasks[i].start,
								ack: true,
							});
							await this.setState(`${message.id}.ACTIONS.schedule.${[i]}.duration`, {
								val: message.attributes.calendar.tasks[i].duration,
								ack: true,
							});
							await this.setState(`${message.id}.ACTIONS.schedule.${[i]}.monday`, {
								val: message.attributes.calendar.tasks[i].monday,
								ack: true,
							});
							await this.setState(`${message.id}.ACTIONS.schedule.${[i]}.tuesday`, {
								val: message.attributes.calendar.tasks[i].tuesday,
								ack: true,
							});
							await this.setState(`${message.id}.ACTIONS.schedule.${[i]}.wednesday`, {
								val: message.attributes.calendar.tasks[i].wednesday,
								ack: true,
							});
							await this.setState(`${message.id}.ACTIONS.schedule.${[i]}.thursday`, {
								val: message.attributes.calendar.tasks[i].thursday,
								ack: true,
							});
							await this.setState(`${message.id}.ACTIONS.schedule.${[i]}.friday`, {
								val: message.attributes.calendar.tasks[i].friday,
								ack: true,
							});
							await this.setState(`${message.id}.ACTIONS.schedule.${[i]}.saturday`, {
								val: message.attributes.calendar.tasks[i].saturday,
								ack: true,
							});
							await this.setState(`${message.id}.ACTIONS.schedule.${[i]}.sunday`, {
								val: message.attributes.calendar.tasks[i].sunday,
								ack: true,
							});
							if (message.attributes.calendar.tasks[i].workAreaId) {
								await this.setState(`${message.id}.ACTIONS.schedule.${[i]}.workAreaId`, {
									val: message.attributes.calendar.tasks[i].workAreaId,
									ack: true,
								});
							}
						}
					}

					if ('position' in message.attributes) {
						if (Object.keys(message.attributes.position).length > 0) {
							this.setState(`${message.id}.positions.positions`, {
								val: JSON.stringify(message.attributes.position),
								ack: true,
							});

							this.setState(`${message.id}.positions.latitude`, {
								val: message.attributes.position.latitude,
								ack: true,
							});
							this.setState(`${message.id}.positions.longitude`, {
								val: message.attributes.position.longitude,
								ack: true,
							});
							this.setState(`${message.id}.positions.latlong`, {
								val: `${message.attributes.position.latitude};${message.attributes.position.longitude}`,
								ack: true,
							});

							// this.log.debug(`[wss.on - message]: message.attributes.positions: ${JSON.stringify(message.attributes.positions)}`);
						}
					}

					if ('battery' in message.attributes) {
						this.setState(`${message.id}.battery.batteryPercent`, {
							val: message.attributes.battery.batteryPercent,
							ack: true,
						});
						// this.log.debug(`[wss.on - message]: message.attributes.battery: ${JSON.stringify(message.attributes.battery)}`);
					}

					if ('mower' in message.attributes) {
						this.setState(`${message.id}.mower.mode`, {
							val: message.attributes.mower.mode,
							ack: true,
						});
						this.setState(`${message.id}.mower.activity`, {
							val: message.attributes.mower.activity,
							ack: true,
						});
						this.setState(`${message.id}.mower.state`, {
							val: message.attributes.mower.state,
							ack: true,
						});
						this.setState(`${message.id}.mower.errorCode`, {
							val: message.attributes.mower.errorCode,
							ack: true,
						});
						this.setState(`${message.id}.mower.errorCodeTimestamp`, {
							val: message.attributes.mower.errorCodeTimestamp,
							ack: true,
						});
						// this.log.debug(`[wss.on - message]: message.attributes.mower: ${JSON.stringify(message.attributes.mower)}`);
					}

					if ('planner' in message.attributes) {
						this.setState(`${message.id}.planner.nextStartTimestamp`, {
							val: message.attributes.planner.nextStartTimestamp,
							ack: true,
						});
						this.setState(`${message.id}.planner.override`, {
							val: message.attributes.planner.override.action,
							ack: true,
						});
						this.setState(`${message.id}.planner.restrictedReason`, {
							val: message.attributes.planner.restrictedReason,
							ack: true,
						});
						this.setState(`${message.id}.planner.externalReason`, {
							val: message.attributes.planner.externalReason,
							ack: true,
						});
						// this.log.debug(`[wss.on - message]: message.attributes.planner: ${JSON.stringify(message.attributes.planner)}`);
					}

					if ('metadata' in message.attributes) {
						this.setState(`${message.id}.metadata.connected`, {
							val: message.attributes.metadata.connected,
							ack: true,
						});
						this.setState(`${message.id}.metadata.statusTimestamp`, {
							val: message.attributes.metadata.statusTimestamp,
							ack: true,
						});
						// this.log.debug(`[wss.on - message]: message.attributes.metadata: ${JSON.stringify(message.attributes.metadata)}`);
					}

					if ('statistics' in message.attributes) {
						this.setState(`${message.id}.statistics.cuttingBladeUsageTime`, {
							val: message.attributes.statistics.cuttingBladeUsageTime,
							ack: true,
						});
						this.setState(`${message.id}.statistics.numberOfChargingCycles`, {
							val: message.attributes.statistics.numberOfChargingCycles,
							ack: true,
						});
						this.setState(`${message.id}.statistics.numberOfCollisions`, {
							val: message.attributes.statistics.numberOfCollisions,
							ack: true,
						});
						this.setState(`${message.id}.statistics.totalChargingTime`, {
							val: message.attributes.statistics.totalChargingTime,
							ack: true,
						});
						this.setState(`${message.id}.statistics.totalCuttingTime`, {
							val: message.attributes.statistics.totalCuttingTime,
							ack: true,
						});
						this.setState(`${message.id}.statistics.totalRunningTime`, {
							val: message.attributes.statistics.totalRunningTime,
							ack: true,
						});
						this.setState(`${message.id}.statistics.totalSearchingTime`, {
							val: message.attributes.statistics.totalSearchingTime,
							ack: true,
						});
						// this.log.debug(`[wss.on - message]: message.attributes.statistics: ${JSON.stringify(message.attributes.statistics)}`);
					}
				} else {
					// do nothing
					// this.log.debug('[wss.on - message]: No values found. Nothing updated');
				}
			} catch (error) {
				this.log.debug(`[wss.on - error]: ${error} (ERR_#010)`);
			}
		});

		//. https://docs.w3cub.com/dom/websocket/close
		this.wss.on('close', async (data, reason) => {
			// https://docs.w3cub.com/dom/closeevent/code
			// this.wss.terminate():					readyState: 3; data: 1006 (Abnormal Closure)
			// this.wss.close():						readyState: 3; data: 1005 (No Status Received)
			// this.wss.close(1000, "Work complete"): 	readyState: 3; data: 1000, reason: Work complete

			// every 2 hour:			this.wss.readyState; 3; data: 1001; reason: Going away -> autoRestart()
			// every 1 day:				this.wss.readyState: 3; data: 1006 (Abnormal Closure) -> getAccessToken() and autoRestart()

			this.log.debug(`[wss.on - close]: this.wss.readyState: ${this.wss.readyState}; data: ${data}; reason: ${reason}`);

			this.ping && this.clearTimeout(this.ping);

			this.setState('info.connection', false, true);

			try {
				if (data === 1000) {
					// do not restart because of shut down of connection from the adapter
					this.log.debug(`[wss.on - close]: ${reason}`);
				} else if (data === 1001) {
					// every 2 hours
					await this.autoRestart();
				} else if (data === 1006) {
					// every 1 day
					await this.getAccessToken();
					await this.autoRestart();
				} else if (data === 1012) {
					// 1012 = Service Restart (The server is terminating the connection because it is restarting)
					await this.getAccessToken();
					await this.autoRestart();
				} else {
					throw new Error('Unknown WebSocket error. (ERR_#011)');
				}
			} catch (error) {
				this.log.debug(`[wss.close - error]: ${error}`);
			}
		});

		// Pong from Server
		this.wss.on('pong', () => {
			this.log.debug('[wss.on - pong]: WebSocket receives pong from server.');
			//this.wsHeartbeat();
		});

		this.wss.on('error', error => {
			this.log.debug(`[wss.on - error]: ${error}`);
		});
	}

	async sendPingToServer() {
		this.log.debug('[sendPingToServer]: WebSocket sends ping to server...');
		this.wss.ping('ping');
		this.ping = this.setTimeout(() => {
			this.sendPingToServer();
		}, 570000); // default: 10min = 600000ms / 9min30s = 570000ms
	}

	async autoRestart() {
		// exponential backoff, capped at 5 minutes: a persistent failure (e.g. an extended Husqvarna outage, or a
		// reconnect that itself immediately fails) used to retry every fixed 5 seconds indefinitely, hammering
		// Husqvarna's servers and flooding the adapter log. Doubles on every call, reset to the 5s base delay on
		// the next successful connection (see the 'open' handler in connectToWS()).
		const delay = this.wsReconnectDelay;
		this.log.debug(`[autoRestart]: WebSocket connection terminated by Husqvarna-Server. Reconnect again in ${Math.round(delay / 1000)} seconds...`);
		this.autoRestartTimeout = this.setTimeout(() => {
			this.connectToWS();
		}, delay);
		this.wsReconnectDelay = Math.min(delay * 2, 300000); // cap: max. 5min = 300000ms
	}

	/**
	 * Is called when adapter shuts down - callback has to be called under any circumstances!
	 *
	 * @param {() => void} callback
	 */
	async onUnload(callback) {
		try {
			// Here you must clear all timeouts or intervals that may still be active

			// invalidating Token
			// NOTE: this used to call DELETE /v1/token/{token} with 'X-Api-Key': this.access_token, which is not a
			// valid request (the access token was sent where the Application Key belongs, and no Authorization
			// header was sent at all) and therefore never actually invalidated anything - the "success" case below
			// was in fact just always hitting the generic 403-from-a-malformed-request error path. The correct,
			// currently documented endpoint is POST /v1/oauth2/revoke with the token authenticated via a Bearer
			// header, see https://developer.husqvarnagroup.cloud/apis/authentication-api#readme
			if (this.access_token) {
				await axios({
					method: 'POST',
					url: 'https://api.authentication.husqvarnagroup.dev/v1/oauth2/revoke',
					headers: {
						'Content-Type': 'application/x-www-form-urlencoded',
						Authorization: `Bearer ${this.access_token}`,
						Accept: '*/*',
					},
					data: `token=${this.access_token}`,
				})
					.then(response => {
						this.log.debug(`[onUnload]: HTTP status response: ${response.status} ${response.statusText}; config: ${JSON.stringify(this.redact(response.config))}; headers: ${JSON.stringify(this.redact(response.headers))}; data: ${JSON.stringify(response.data)}`);
						this.log.info('"Husqvarna Authentication API Access token" successfully invalidated.');
					})
					.catch(error => {
						this.logAxiosError('onUnload', error);
					});
			}

			this.autoRestartTimeout && this.clearTimeout(this.autoRestartTimeout);
			this.ping && this.clearTimeout(this.ping);

			this.statisticsInterval && this.clearInterval(this.statisticsInterval);

			this.setState('info.connection', false, true);

			callback();
			this.log.info('cleaned everything up... (#1)');
		} catch {
			callback();
			this.log.info('cleaned everything up... (#2)');
		}
	}

	/**
	 * Is called if a subscribed state changes
	 *
	 * @param {string} id
	 * @param {ioBroker.State | null | undefined} state
	 */
	async onStateChange(id, state) {
		if (state !== null && state !== undefined) {
			if (state.ack === false) {
				// https://developer.husqvarnagroup.cloud/apis/Automower+Connect+API#/readme
				this.log.debug(`[onStateChange]: id: ${id}; state: ${JSON.stringify(state)}`);
				const idSplit = id.split('.');
				// this.log.debug(`[onStateChange]: idSplit.length: ${idSplit.length}`);
				const mowerId = idSplit[2];
				// this.log.debug(`[onStateChange]: mowerId: ${mowerId}`);

				let command = null;
				let parentPath = null;
				if (idSplit.length === 5) {
					command = idSplit[4];
					parentPath = idSplit.slice(0, idSplit.length - 1).join('.');
				} else if (idSplit.length === 6) {
					command = idSplit[5];
					parentPath = idSplit.slice(0, idSplit.length - 2).join('.');
				}
				this.log.debug(`[onStateChange]: command: ${command}`);
				this.log.debug(`[onStateChange]: parentPath: ${parentPath}`);

				const data_command = {};
				let url = '';
				let method = 'POST';
				const data_tasks = [];

				if (command === 'START') {
					const startTime = await this.getStateAsync(`${parentPath}.start.startTime`);
					if (startTime && startTime.val) {
						if (Number(startTime.val) >= 0 && Number(startTime.val) <= 1439) {
							// NOTE: "attributes" must be nested INSIDE "data" (data.attributes), not a sibling of "data" -
							// a sibling "attributes" key is silently ignored resp. rejected by the API
							data_command.data = { type: 'Start', attributes: { duration: Number(startTime.val) } };
							url = 'actions';
						} else {
							this.log.error('Inputvalue "startTime" not valid. Nothing Set. (ERR_#016');
							return;
						}
					} else {
						this.log.error('Missing "startTime". Nothing Set. (ERR_#017');
						return;
					}
				} else if (command === 'STARTINWORKAREA') {
					const startTime = await this.getStateAsync(`${parentPath}.StartInWorkArea.startTime`);
					const workAreaId = await this.getStateAsync(`${parentPath}.StartInWorkArea.workAreaId`);
					if (startTime && startTime.val) {
						if (Number(startTime.val) >= 0 && Number(startTime.val) <= 1439) {
							if (workAreaId && workAreaId.val) {
								if (Number(workAreaId.val) > 0) {
									data_command.data = { type: 'StartInWorkArea', attributes: { duration: Number(startTime.val), workAreaId: Number(workAreaId.val) } };
									url = 'actions';
								} else {
									this.log.error('Missing "workAreaId". Nothing Set. (ERR_#018');
									return;
								}
							} else {
								this.log.error('Inputvalue "workAreaId" not valid. Nothing Set. (ERR_#019');
								return;
							}
						} else {
							this.log.error('Inputvalue "startTime" not valid. Nothing Set. (ERR_#020');
							return;
						}
					} else {
						this.log.error('Missing "startTime". Nothing Set. (ERR_#021');
						return;
					}
				} else if (command === 'RESUMESCHEDULE') {
					data_command.data = { type: 'ResumeSchedule' };
					url = 'actions';
				} else if (command === 'PAUSE') {
					data_command.data = { type: 'Pause' };
					url = 'actions';
				} else if (command === 'PARK') {
					const parkTime = await this.getStateAsync(`${parentPath}.park.parkTime`);
					if (parkTime && parkTime.val) {
						if (Number(parkTime.val) > 0) {
							data_command.data = { type: 'Park', attributes: { duration: Number(parkTime.val) } };
							url = 'actions';
						} else {
							this.log.error('Inputvalue "parkTime" not valid. Nothing Set. (ERR_#022');
							return;
						}
					} else {
						this.log.error('Missing "parkTime". Nothing Set. (ERR_#023');
						return;
					}
				} else if (command === 'PARKUNTILNEXTSCHEDULE') {
					data_command.data = { type: 'ParkUntilNextSchedule' };
					url = 'actions';
				} else if (command === 'PARKUNTILFURTHERNOTICE') {
					data_command.data = { type: 'ParkUntilFurtherNotice' };
					url = 'actions';
				} else if (command === 'CUTTINGHEIGHT') {
					if (Number(state.val) >= 1 && Number(state.val) <= 9) {
						data_command.data = { type: 'settings', attributes: { cuttingHeight: Number(state.val) } };
						url = 'settings';
					} else {
						this.log.error('Inputvalue "CUTTINGHEIGHT" not valid. Nothing Set. (ERR_#024');
						return;
					}
				} else if (command === 'DATETIME') {
					if (Number(state.val) > 1725141600) {
						// NOTE: "type" must be "settings" (like every other settings-endpoint command), not "dateTime" -
						// the API rejects an unknown "type" value with a 400 error
						data_command.data = { type: 'settings', attributes: { dateTime: Number(state.val) } };
						url = 'settings';
					} else {
						this.log.error('Inputvalue "DATETIME" not valid. Nothing Set. (ERR_#025');
						return;
					}
				} else if (command === 'HEADLIGHT') {
					// NOTE: valid value is "ALWAYS_OFF" (underscore), not "ALWAYS OFF" (space) - the previous check could
					// therefore never match this option and always fell through to the error branch below
					if (state.val === 'ALWAYS_ON' || state.val === 'ALWAYS_OFF' || state.val === 'EVENING_ONLY' || state.val === 'EVENING_AND_NIGHT') {
						// NOTE: "type" must be "settings" and the mode goes into a nested "headlight" object, not
						// flat as "attributes.mode" - see set_headlight_mode() in Husqvarna's own aioautomower client.
						data_command.data = { type: 'settings', attributes: { headlight: { mode: state.val } } };
						url = 'settings';
					} else {
						this.log.error('Inputvalue "HEADLIGHT" not valid. Nothing Set. (ERR_#026');
						return;
					}
				} else if (command === 'CONFIRMERROR') {
					// POST .../errors/confirm - confirms a non-fatal error (only works if mower.isErrorConfirmable is true).
					// No request body required/expected.
					url = 'errors/confirm';
				} else if (command === 'RESETCUTTINGBLADEUSAGETIME') {
					// POST .../statistics/resetCuttingBladeUsageTime - resets statistics.cuttingBladeUsageTime to 0.
					// No request body required/expected.
					url = 'statistics/resetCuttingBladeUsageTime';
				} else if (command === 'APPLYWORKAREASETTINGS') {
					const workAreaId = await this.getStateAsync(`${parentPath}.workAreaSettings.workAreaId`);
					const cuttingHeight = await this.getStateAsync(`${parentPath}.workAreaSettings.cuttingHeight`);
					const enabled = await this.getStateAsync(`${parentPath}.workAreaSettings.enabled`);
					if (workAreaId && workAreaId.val !== null && workAreaId.val !== undefined && Number(workAreaId.val) >= 0) {
						const attributes = {};
						if (cuttingHeight && cuttingHeight.val !== null && cuttingHeight.val !== undefined) {
							if (Number(cuttingHeight.val) >= 0 && Number(cuttingHeight.val) <= 100) {
								attributes.cuttingHeight = Number(cuttingHeight.val);
							} else {
								this.log.error('Inputvalue "workAreaSettings.cuttingHeight" not valid. Nothing Set. (ERR_#027');
								return;
							}
						}
						if (enabled && enabled.val !== null && enabled.val !== undefined) {
							attributes.enable = Boolean(enabled.val);
						}
						if (Object.keys(attributes).length === 0) {
							this.log.error('Neither "workAreaSettings.cuttingHeight" nor "workAreaSettings.enabled" set. Nothing Set. (ERR_#028');
							return;
						}
						// PATCH .../workAreas/{workAreaId}
						data_command.data = { type: 'workArea', id: Number(workAreaId.val), attributes };
						url = `workAreas/${Number(workAreaId.val)}`;
						method = 'PATCH';
					} else {
						this.log.error('Missing/invalid "workAreaSettings.workAreaId". Nothing Set. (ERR_#029');
						return;
					}
				} else if (command === 'APPLYSTAYOUTZONESETTINGS') {
					const zoneId = await this.getStateAsync(`${parentPath}.stayOutZoneSettings.zoneId`);
					const enabled = await this.getStateAsync(`${parentPath}.stayOutZoneSettings.enabled`);
					if (zoneId && zoneId.val) {
						// PATCH .../stayOutZones/{zoneId}
						data_command.data = { type: 'stayOutZone', id: zoneId.val, attributes: { enable: Boolean(enabled && enabled.val) } };
						// encodeURIComponent(): zoneId is a free-text string state (unlike workAreaId, which is numeric and
						// therefore safe by construction) - never interpolate it into a URL unencoded
						url = `stayOutZones/${encodeURIComponent(zoneId.val)}`;
						method = 'PATCH';
					} else {
						this.log.error('Missing "stayOutZoneSettings.zoneId". Nothing Set. (ERR_#030');
						return;
					}
				} else if (command === 'SET') {
					for (let i = 0; i < this.capabilities.find(o => o.id === mowerId).numbersOfCalendars; i++) {
						const scheduleStart = await this.getStateAsync(`${parentPath}.schedule.${i}.start`);
						const scheduleDuration = await this.getStateAsync(`${parentPath}.schedule.${i}.duration`);
						const scheduleMonday = await this.getStateAsync(`${parentPath}.schedule.${i}.monday`);
						const scheduleThuesday = await this.getStateAsync(`${parentPath}.schedule.${i}.tuesday`);
						const scheduleWednesday = await this.getStateAsync(`${parentPath}.schedule.${i}.wednesday`);
						const scheduleThursday = await this.getStateAsync(`${parentPath}.schedule.${i}.thursday`);
						const scheduleFriday = await this.getStateAsync(`${parentPath}.schedule.${i}.friday`);
						const scheduleSaturday = await this.getStateAsync(`${parentPath}.schedule.${i}.saturday`);
						const scheduleSunday = await this.getStateAsync(`${parentPath}.schedule.${i}.sunday`);
						if (this.capabilities.find(o => o.id === mowerId).workAreas) {
							const scheduleWorkAreaId = await this.getStateAsync(`${parentPath}.schedule.${i}.workAreaId`);
							if (scheduleStart && scheduleDuration && scheduleMonday && scheduleThuesday && scheduleWednesday && scheduleThursday && scheduleFriday && scheduleSaturday && scheduleSunday && scheduleWorkAreaId) {
								if ((scheduleMonday.val || scheduleThuesday.val || scheduleWednesday.val || scheduleThursday.val || scheduleFriday.val || scheduleSaturday.val || scheduleSunday.val) && scheduleWorkAreaId.val) {
									data_tasks.push({
										start: scheduleStart.val,
										duration: scheduleDuration.val,
										monday: scheduleMonday.val,
										tuesday: scheduleThuesday.val,
										wednesday: scheduleWednesday.val,
										thursday: scheduleThursday.val,
										friday: scheduleFriday.val,
										saturday: scheduleSaturday.val,
										sunday: scheduleSunday.val,
										workAreaId: scheduleWorkAreaId.val,
									});
								} else {
									this.log.error('Inputvalue "SCHEDULE" not valid. Nothing Set. (ERR_#031');
									return;
								}
							} else {
								this.log.error('Inputvalue "SCHEDULE" not valid. Nothing Set. (ERR_#032');
								return;
							}
							url = `workAreas/${Number(scheduleWorkAreaId.val)}/calendar`;
						} else {
							if (scheduleStart && scheduleDuration && scheduleMonday && scheduleThuesday && scheduleWednesday && scheduleThursday && scheduleFriday && scheduleSaturday && scheduleSunday) {
								if (scheduleMonday.val || scheduleThuesday.val || scheduleWednesday.val || scheduleThursday.val || scheduleFriday.val || scheduleSaturday.val || scheduleSunday.val) {
									data_tasks.push({
										start: scheduleStart.val,
										duration: scheduleDuration.val,
										monday: scheduleMonday.val,
										tuesday: scheduleThuesday.val,
										wednesday: scheduleWednesday.val,
										thursday: scheduleThursday.val,
										friday: scheduleFriday.val,
										saturday: scheduleSaturday.val,
										sunday: scheduleSunday.val,
									});
								} else {
									this.log.error('Inputvalue "SCHEDULE" not valid. Nothing Set. (ERR_#033');
									return;
								}
							} else {
								this.log.error('Inputvalue "SCHEDULE" not valid. Nothing Set. (ERR_#034');
								return;
							}
							url = 'calendar';
						}
					}
					data_command.data = { type: 'calendar' };
					data_command.data.attributes = { tasks: data_tasks };
					// this.log.debug(`[onStateChange]: data_command: ${JSON.stringify(data_command)}`);
				} else if (command === 'REFRESHSTATISTICS') {
					try {
						await this.getMowerData();
						await this.fillObjects(this.mowerData);
						await this.getAndFillMowerMessages();
					} catch (error) {
						this.log.debug(`${error} (ERR_#035)`);
					}
					return;
				}

				this.log.debug(`method: ${method}; url: https://api.amc.husqvarna.dev/v1/mowers/${mowerId}/${url}`);
				this.log.debug(`data: ${JSON.stringify(data_command)}`);

				await axios({
					method: method,
					url: `https://api.amc.husqvarna.dev/v1/mowers/${mowerId}/${url}`,
					headers: {
						Authorization: `Bearer ${this.access_token}`,
						'X-Api-Key': this.config.applicationKey,
						'Authorization-Provider': 'husqvarna',
						'Content-Type': 'application/vnd.api+json',
					},
					// CONFIRMERROR / RESETCUTTINGBLADEUSAGETIME need no request body at all (data_command.data stays unset)
					data: 'data' in data_command ? data_command : undefined,
				})
					.then(response => {
						this.log.debug(`[onStateChange]: HTTP status response: ${response.status} ${response.statusText}; config: ${JSON.stringify(this.redact(response.config))}; headers: ${JSON.stringify(this.redact(response.headers))}; data: ${JSON.stringify(response.data)}`);
						if (response.status === 202) {
							this.log.info(`${response.statusText}. Command ${command} Set.`);
						}
					})
					.catch(async error => {
						this.logAxiosError('onStateChange', error);
						if (error.response) {
							if (error.response.status === 400) {
								// Invalid schedule format in request body. Parsing message: No tasks.
								this.log.info(`${error.response.data.errors[0].detail} Nothing set.`);
							} else if (error.response.status === 403) {
								// The supplied credentials are invalid (accesstoken no longer valid)
								try {
									await this.getAccessToken();
									await this.autoRestart();
								} catch (error) {
									this.log.error(`${error} (ERR_#014)`);
								}
								// TODO (if needed): send command again
							} else if (error.response.status === 404) {
								// No connection between the cloud service and the mower.
								this.setState(`${mowerId}.metadata.connected`, { val: false, ack: true });
								this.setState(`${mowerId}.metadata.statusTimestamp`, {
									val: new Date().getTime(),
									ack: true,
								});
								this.log.info(`${error.response.data.errors[0].detail} Nothing set.`);
							}
						}
					});
			} else {
				// The state was changed by system
				this.log.debug(`[onStateChange]: state changed by system: ${id}; changed: ${state.val}; (ack = ${state.ack}). NO ACTION PERFORMED.`);
			}
		} else {
			// The state was deleted
			this.log.debug(`[onStateChange]: state unknown: ${id} was changed. NO ACTION PERFORMED.`);
		}
	}
}

if (require.main !== module) {
	// Export the constructor in compact mode
	/**
	 * @param {Partial<utils.AdapterOptions>} [options={}]
	 */
	module.exports = options => new HusqvarnaAutomower(options);
} else {
	// otherwise start the instance directly
	new HusqvarnaAutomower();
}
