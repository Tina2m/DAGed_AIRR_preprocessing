# Unused Code Analysis Report

## Summary
This document identifies unused files, functions, CSS rules, and imports across the project.

## 🔴 Unused Files

### Definitely Unused
1. **`placeholder.txt`** - Contains only "temp", appears to be a temporary file
2. **`python`** - Contains only "python placeholder", appears to be a placeholder file
3. **`app/main_refactored.py`** - Template file for refactoring, not used in production

### Potentially Unused (Documentation)
4. **`REFACTORING_SUMMARY.md`** - Documentation file (keep for reference)
5. **`REFACTORING_COMPLETE.md`** - Documentation file (keep for reference)
6. **`STYLE_ENFORCEMENT_SUMMARY.md`** - Documentation file (keep for reference)
7. **`STYLE_ENFORCEMENT_COMPLETE.md`** - Documentation file (keep for reference)
8. **`STYLE_GUIDE.md`** - Documentation file (keep for reference)

**Recommendation**: Delete `placeholder.txt` and `python` files. Keep documentation files.

## 🟡 Unused Python Imports

### In `app/main.py`
1. **`json`** - Imported but never used
   - Line 2: `import os, json, uuid, gzip, pathlib, shutil, subprocess`
   - No usage found: `grep "json\."` returns no matches

### Unused Functions
2. **`_ensure_uncompressed_path()`** - Defined but never called
   - Line 62: Function definition
   - Only appears in its own definition, no call sites found

**Recommendation**: Remove `json` import. Keep `_ensure_uncompressed_path()` for potential future use or remove if confirmed unused.

## 🟠 Unused JavaScript Functions

### In `ui/assets/js/script.js`

#### Legacy DAG Functions (Replaced by bulk.js)
These functions use `BULK_DAG` but are superseded by `bulk.js` which uses `DAG_STATE` and provides `window.BulkDag` API:

1. **`serializeDag()`** - Defined but never called
   - Line 83: Function definition
   - No call sites found

2. **`hydrateDag(payload)`** - Defined but never called
   - Line 90: Function definition
   - No call sites found

3. **`dagAddNode(unitId, options)`** - Defined but never called
   - Line 42: Function definition
   - Replaced by `bulk.js` `addNode()` via `window.BulkDag.createNode()`

4. **`dagRemoveNode(nodeId)`** - Defined but never called
   - Line 55: Function definition
   - Replaced by `bulk.js` `removeNode()`

5. **`dagConnect(fromId, toId, channel)`** - Defined but never called
   - Line 62: Function definition
   - Replaced by `bulk.js` `connectNodes()`

6. **`dagDisconnect(fromId, toId)`** - Defined but never called
   - Line 69: Function definition
   - Replaced by `bulk.js` `removeEdge()`

7. **`dagIncoming(nodeId)`** - Defined but never called
   - Line 75: Function definition
   - No call sites found

8. **`dagOutgoing(nodeId)`** - Used internally in `dagTopoOrder()`
   - Line 79: Function definition
   - **KEEP** - Used in `dagTopoOrder()` at line 125

9. **`dagTopoOrder()`** - Used internally
   - Line 110: Function definition
   - **KEEP** - Used in validation

#### Variables
10. **`BULK_DAG`** - Legacy variable, replaced by `bulk.js` `DAG_STATE`
    - Line 5: `let BULK_DAG = createEmptyBulkDag();`
    - Only used by unused functions above

11. **`UNIT_GROUP`** - Constant defined but never used
    - Line 11: `const UNIT_GROUP = 'bulk';`
    - No usage found

**Recommendation**: Remove unused DAG functions and `BULK_DAG` variable. They're replaced by `bulk.js` implementation.

## 🟡 Unused CSS Classes

### In `ui/assets/css/bulk.css`
1. **`.upload-grid`** - Referenced in HTML but no CSS rule defined
   - HTML: `ui/blk.html` line 86: `<div class="grid upload-grid">`
   - CSS: No matching rule found

2. **`.palette-section`** - Referenced in HTML but no CSS rule defined
   - HTML: `ui/blk.html` line 116: `<div class="dag-only palette-section">`
   - CSS: No matching rule found

3. **`.card-divider`** - CSS rule exists but class never used in HTML/JS
   - CSS: `bulk.css` line 70-74
   - No usage in HTML or JavaScript

4. **`.params-panel`** - CSS rule exists but class never used
   - CSS: `bulk.css` line 75-79
   - No usage in HTML or JavaScript (might be dynamically created)

**Recommendation**: 
- Remove unused CSS rules for `.card-divider` and `.params-panel` if confirmed unused
- Add CSS rules for `.upload-grid` and `.palette-section` if needed, or remove from HTML

## ✅ Used Functions (Keep These)

### JavaScript Functions That ARE Used
- `createEmptyBulkDag()` - Used to initialize `BULK_DAG`
- `dagNodeId()` - Used in `dagAddNode()`
- `unitChannels()` - Used in DAG functions
- `dagOutgoing()` - Used in `dagTopoOrder()`
- `dagTopoOrder()` - Used in validation
- All other functions in `script.js` are actively used

## 📊 Statistics

### Unused Code Found
- **Files**: 2 (placeholder.txt, python)
- **Python imports**: 1 (json)
- **Python functions**: 1 (`_ensure_uncompressed_path`)
- **JavaScript functions**: 6-7 (DAG functions in script.js)
- **JavaScript variables**: 2 (BULK_DAG, UNIT_GROUP)
- **CSS classes**: 4 (upload-grid, palette-section, card-divider, params-panel)

### Total Lines of Unused Code
- Estimated: ~150-200 lines of unused code

## 🎯 Recommendations

### High Priority (Safe to Remove)
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
   - `dagIncoming()` (if not used)
5. ✅ Remove `BULK_DAG` variable
6. ✅ Remove `UNIT_GROUP` constant

### Medium Priority (Verify First)
1. ⚠️ Remove `_ensure_uncompressed_path()` if confirmed unused
2. ⚠️ Remove unused CSS rules (`.card-divider`, `.params-panel`)
3. ⚠️ Add CSS for `.upload-grid` and `.palette-section` or remove from HTML

### Low Priority (Documentation)
1. 📝 Keep documentation files for reference
2. 📝 Consider archiving `main_refactored.py` or moving to docs folder

## ⚠️ Notes

### DAG Implementation
The DAG functionality appears to have been refactored:
- **Old**: `script.js` uses `BULK_DAG` variable and local functions
- **New**: `bulk.js` uses `DAG_STATE` and `window.BulkDag` API
- The old implementation in `script.js` is now redundant

### CSS Classes
Some CSS classes might be dynamically created by JavaScript:
- `.params-panel` might be created dynamically
- Verify with browser DevTools before removing

## Next Steps

1. **Verify unused code** with browser DevTools and runtime analysis
2. **Create backup** before removing code
3. **Remove unused code** systematically
4. **Test functionality** after each removal
5. **Update documentation** if needed

