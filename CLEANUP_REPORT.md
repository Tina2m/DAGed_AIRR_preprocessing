# Unused Code Cleanup Report

## Executive Summary

Scanned the entire project and identified **unused files, functions, CSS rules, and imports**. This report provides a comprehensive analysis with recommendations for cleanup.

## 🔴 Unused Files (Safe to Delete)

1. **`placeholder.txt`** - Temporary file containing only "temp"
2. **`python`** - Placeholder file containing only "python placeholder"

**Action**: Delete these files immediately.

## 🟡 Unused Python Code

### In `app/main.py`

1. **Unused Import**: `json`
   - Line 2: `import os, json, uuid, gzip, pathlib, shutil, subprocess`
   - **Status**: Never used in the file
   - **Action**: Remove from import statement

2. **Unused Function**: `_ensure_uncompressed_path()`
   - Line 62-69: Function definition
   - **Status**: Defined but never called
   - **Action**: Remove if confirmed unused, or keep for future use

## 🟠 Unused JavaScript Code

### In `ui/assets/js/script.js`

#### Legacy DAG Functions (Replaced by `bulk.js`)

The following functions use the old `BULK_DAG` variable but are completely replaced by `bulk.js` which uses `DAG_STATE` and provides `window.BulkDag` API:

1. **`serializeDag()`** - Line 83
   - **Status**: Never called
   - **Replaced by**: `bulk.js` → `window.BulkDag.serialize()`

2. **`hydrateDag(payload)`** - Line 90
   - **Status**: Never called
   - **Replaced by**: Not needed (bulk.js manages state internally)

3. **`dagAddNode(unitId, options)`** - Line 42
   - **Status**: Never called
   - **Replaced by**: `bulk.js` → `window.BulkDag.createNode()`

4. **`dagRemoveNode(nodeId)`** - Line 55
   - **Status**: Never called
   - **Replaced by**: `bulk.js` → `removeNode()`

5. **`dagConnect(fromId, toId, channel)`** - Line 62
   - **Status**: Never called
   - **Replaced by**: `bulk.js` → `connectNodes()`

6. **`dagDisconnect(fromId, toId)`** - Line 69
   - **Status**: Never called
   - **Replaced by**: `bulk.js` → `removeEdge()`

7. **`dagIncoming(nodeId)`** - Line 75
   - **Status**: Never called
   - **Note**: Could be useful but not currently used

8. **`dagOutgoing(nodeId)`** - Line 79
   - **Status**: Only used in `dagTopoOrder()` which is also unused
   - **Replaced by**: `bulk.js` → `topoOrder()` (different implementation)

9. **`dagTopoOrder()`** - Line 110
   - **Status**: Never called (validation uses `bulk.js` API)
   - **Replaced by**: `bulk.js` → `window.BulkDag.topoOrder()`

#### Supporting Functions (Also Unused)

10. **`createEmptyBulkDag()`** - Line 30
    - **Status**: Only used to initialize `BULK_DAG` which is unused
    - **Action**: Remove with `BULK_DAG`

11. **`dagNodeId()`** - Line 34
    - **Status**: Only used in `dagAddNode()` which is unused
    - **Action**: Remove

12. **`unitChannels(unitId)`** - Line 38
    - **Status**: Only used in unused DAG functions
    - **Note**: `CHANNEL_MAP` is still used by `syncDagMetaFromUnits()`
    - **Action**: Keep `CHANNEL_MAP`, remove function

#### Unused Variables

13. **`BULK_DAG`** - Line 5
    - **Status**: Only used by unused functions above
    - **Replaced by**: `bulk.js` → `DAG_STATE`

14. **`UNIT_GROUP`** - Line 11
    - **Status**: Constant defined but never used
    - **Action**: Remove

#### Functions That ARE Used (Keep These)

✅ **Keep**: `availableDagFiles()`, `guessChannel()`, `fillDagSelect()`, `refreshDagFileSelects()`, `determineDagBranch()`, `decorateDagControls()` - All used by DAG controls
✅ **Keep**: `syncDagMetaFromUnits()` - Used to sync metadata to bulk.js
✅ **Keep**: `validateDagPipeline()` - Uses `bulk.js` API, not local functions
✅ **Keep**: `CHANNEL_MAP` - Used by `syncDagMetaFromUnits()`

## 🟡 Unused CSS Rules

### In `ui/assets/css/bulk.css`

1. **`.card-divider`** - Lines 70-74
   - **Status**: CSS rule exists but class never used in HTML/JS
   - **Action**: Remove if confirmed unused

2. **`.params-panel`** - Lines 75-79
   - **Status**: CSS rule exists but class never used
   - **Note**: Might be dynamically created - verify with DevTools
   - **Action**: Verify before removing

### Missing CSS Rules (Referenced in HTML but not defined)

3. **`.upload-grid`** - Referenced in `blk.html` line 86
   - **Status**: Class used in HTML but no CSS rule
   - **Action**: Add CSS rule or remove from HTML

4. **`.palette-section`** - Referenced in `blk.html` line 116
   - **Status**: Class used in HTML but no CSS rule
   - **Action**: Add CSS rule or remove from HTML

## 📊 Statistics

### Code to Remove
- **Files**: 2
- **Python imports**: 1 (`json`)
- **Python functions**: 1 (`_ensure_uncompressed_path`)
- **JavaScript functions**: 11 (DAG-related)
- **JavaScript variables**: 2 (`BULK_DAG`, `UNIT_GROUP`)
- **CSS rules**: 2-4 (depending on verification)

### Estimated Lines to Remove
- **Total**: ~150-200 lines of unused code

## 🎯 Cleanup Recommendations

### High Priority (Safe to Remove Immediately)

1. ✅ Delete `placeholder.txt`
2. ✅ Delete `python` file
3. ✅ Remove `json` import from `app/main.py`
4. ✅ Remove unused DAG functions from `script.js`:
   - `serializeDag()`
   - `hydrateDag()`
   - `dagAddNode()`
   - `dagRemoveNode()`
   - `dagConnect()`
   - `dagDisconnect()`
   - `dagIncoming()`
   - `dagOutgoing()`
   - `dagTopoOrder()`
   - `createEmptyBulkDag()`
   - `dagNodeId()`
   - `unitChannels()` (if CHANNEL_MAP is kept separately)
5. ✅ Remove `BULK_DAG` variable
6. ✅ Remove `UNIT_GROUP` constant

### Medium Priority (Verify First)

1. ⚠️ Remove `_ensure_uncompressed_path()` if confirmed unused
2. ⚠️ Verify and remove unused CSS rules (`.card-divider`, `.params-panel`)
3. ⚠️ Add CSS for `.upload-grid` and `.palette-section` or remove from HTML

### Low Priority

1. 📝 Consider archiving documentation files or moving to `docs/` folder
2. 📝 Consider archiving `app/main_refactored.py` or moving to `docs/` folder

## ⚠️ Important Notes

### DAG Implementation Migration
The DAG functionality has been refactored:
- **Old Implementation** (in `script.js`): Uses `BULK_DAG` variable and local functions
- **New Implementation** (in `bulk.js`): Uses `DAG_STATE` and `window.BulkDag` API
- **Status**: Old implementation is completely redundant and can be safely removed

### CSS Classes
Some classes might be dynamically created:
- Verify with browser DevTools before removing CSS rules
- Check JavaScript for `classList.add()` or `className =` assignments

## 🔍 Verification Steps

Before removing code:
1. ✅ Search codebase for function/variable usage
2. ✅ Check browser DevTools for dynamically created elements
3. ✅ Test application functionality after each removal
4. ✅ Keep backups before major cleanup

## Next Steps

1. Review this report
2. Verify findings with runtime analysis
3. Create backup branch
4. Remove unused code systematically
5. Test thoroughly after cleanup

