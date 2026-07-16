# Older changes

Changelog entries for releases before 1.0.0 (the rename to husqvarna-automower-connect).
Full history and issue links refer to the original project, [ice987987/ioBroker.husqvarna-automower](https://github.com/ice987987/ioBroker.husqvarna-automower).

### 0.6.0-beta.12 **WORK IN PROGRESS**

-   (ice987987) BREAKING: admin >= v7.4.10 is required
-   (ice987987) states `.capabilities.canConfirmError`, `mower.workAreaId`, `.workAreas.[workAreaId].enabled`, `.workAreas.[workAreaId].lastTimeCompleted`, `.workAreas.[workAreaId].progress`,`ACTIONS.REFRESHSTATISTICS` and `.ACTIONS.DATETIME` added
-   (ice987987) state `.workAreas.[workAreaId].calendar` removed
-   (ice987987) state `.ACTIONS.schedule.[i].workAreaId` added, if supportet by the model
-   (ice987987) schedule-limitation removed `.ACTIONS.schedule.[i].`
-   (ice987987) state `.positions.positions` added [#191](https://github.com/ice987987/ioBroker.husqvarna-automower/issues/191)
-   (ice987987) try to fix [#197](https://github.com/ice987987/ioBroker.husqvarna-automower/issues/197), [#226](https://github.com/ice987987/ioBroker.husqvarna-automower/issues/226), [#228](https://github.com/ice987987/ioBroker.husqvarna-automower/issues/228), [#230](https://github.com/ice987987/ioBroker.husqvarna-automower/issues/230), [#231](https://github.com/ice987987/ioBroker.husqvarna-automower/issues/231)
-   (ice987987) dependencies updated

### 0.5.0 (17.02.2025)

-   (ice987987) BREAKING: js-controller >= v5.0.19 and node >= v18 is required
-   (ice987987) BREAKING: `.planner.action` removed
-   (ice987987) state `.ACTIONS.startInWorkArea.STARTINWORKAREA` start mower and cut for a duration of time `.ACTIONS.startInWorkArea.duration` (in minutes, optional, if zero (0) the override will be forever), in Area with ID `.ACTIONS.startInWorkArea.workAreaId` added [#124](https://github.com/ice987987/ioBroker.husqvarna-automower/issues/124)
-   (ice987987) states `.capabilities.position`, `.capabilities.headlights`, `.capabilities.workAreas`, `.capabilities.stayOutZones`, `.planner.externalReason`, `.stayOutZones.dirty`, `.stayOutZones.zones`, `.statistics.totalDrivenDistance`, `.workAreas.[workAreaId].workAreaId`, `.workAreas.[workAreaId].name`, `.workAreas.[workAreaId].cuttingHeight` and `.workAreas.[workAreaId].calendar` added [#124](https://github.com/ice987987/ioBroker.husqvarna-automower/issues/124)
-   (ice987987) state `.mower.inactiveReason` added
-   (ice987987) names of several states updated
-   (ice987987) descriptions of status and error codes updated
-   (ice987987) dependencies updated
-   (ice987987) source code improvements
-   (ice987987) state description of `.mower.errorCode` updated
-   (ice987987) state `.statistics.totalDriveDistance` corrected [#162](https://github.com/ice987987/ioBroker.husqvarna-automower/issues/162)
-   (ice987987) try to fix [#164](https://github.com/ice987987/ioBroker.husqvarna-automower/issues/164)
-   (ice987987) try to fix [#182](https://github.com/ice987987/ioBroker.husqvarna-automower/issues/182)

### 0.4.0 (07.07.2023)

-   (ice987987) BREAKING: `.settings.cuttingHeight` and `.settings.headlight` removed [#99](https://github.com/ice987987/ioBroker.husqvarna-automower/issues/99)
-   (ice987987) BREAKING: `.calendar.[0-3].start`, `.calendar.[0-3].duration`, `.calendar.[0-3].monday`, `.calendar.[0-3].tuesday`, `.calendar.[0-3].wednesday`, `.calendar.[0-3].thurdsay`, `.calendar.[0-3].friday`, `.calendar.[0-3].saturday` and `.calendar.[0-3].sunday` removed
-   (ice987987) BREAKING: node.js >= v16.4 and js-controller >= v4.0.24
-   (ice987987) dependencies updated
-   (ice987987) adapter icon updated
-   (ice987987) script for statistics updated

### 0.3.3 (11.05.2023)

-   (MK-2001) simple check if response contains geo data added [#98](https://github.com/ice987987/ioBroker.husqvarna-automower/issues/98)
-   (ice987987) dependencies updated

### 0.3.2 (30.03.2023)

-   (ice987987) BREAKING: admin >= v6.3.5 is required
-   (ice987987) section "disclaimer" in readme added
-   (ice987987) ukrainian language added

### 0.3.1 (04.11.2022)

-   (ice987987) BREAKING: js-controller >= v4.0.23 and admin >= v6.2.19 is required
-   (ice987987) source code improvements
-   (ice987987) ability to update statistical values
-   (ice987987) update dependencies
-   (ice987987) restructure feature request form
-   (ice987987) fix issue [#65](https://github.com/ice987987/ioBroker.husqvarna-automower/issues/65)

### 0.3.0 (08.08.2022)

-   (ice987987) improved logging
-   (ice987987) update dependencies
-   (ice987987) update of vis binding `husqvarna-automower-connect.0.[mowerID from DP .system.id].mower.errorCode`
-   (ice978987) update of `common.states` of `.mower.errorCode`
-   (ice987987) adding German translations of vis-Bindings
-   (ice987987) adding Javascript-Script for statistics (`Distance between mower and charging station`)
-   (ice987987) adding statistics values from the Automower Connect API `.statistics.cuttingBladeUsageTime`, `.statistics.numberOfChargingCycles`, `.statistics.numberOfCollisions`, `.statistics.totalChargingTime`, `.statistics.totalCuttingTime`, `.statistics.totalRunningTime` and `.statistics.totalSearchingTime`
-   (ice987987) adding feature request form

### 0.2.0 (14.06.2022)

-   (ice987987) support new login procedure to husqvarna's webservice using "Application key" and "Application secret" instead of "username (emailadress)" and "password" (issue [#33](https://github.com/ice987987/ioBroker.husqvarna-automower/issues/33))
-   (ice987987) update links to husqvarna homepage due to their updated homepage
-   (ice987987) improved bug-report form
