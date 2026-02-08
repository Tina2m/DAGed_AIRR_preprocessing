"""Processing units for bulk and single-cell workflows."""
from .base import UnitSpec
from .bulk_units import (
    FilterQualityUnit,
    FilterLengthUnit,
    FilterMissingUnit,
    FilterRepeatsUnit,
    FilterTrimQualUnit,
    FilterMaskQualUnit,
    MaskPrimersUnit,
    CollapseSeqUnit,
    BuildConsensusUnit,
)
from .sc_units import (
    MergeSamplesUnit,
    SC_FilterProductiveUnit,
    SC_RemoveMultiHeavyUnit,
    SC_RemoveNoHeavyUnit,
)

# Unit registry
UNITS = {
    "filter_quality": FilterQualityUnit(
        id="filter_quality",
        label="FilterSeq: quality",
        requires=["R1"],
        group="bulk",
        params_schema={"qmin": {"type": "int", "default": 20, "min": 0, "max": 40}}
    ),
    "filter_length": FilterLengthUnit(
        id="filter_length",
        label="FilterSeq: length",
        requires=["R1"],
        group="bulk",
        params_schema={
            "min_len": {"type": "int", "default": 100, "min": 1},
            "inner": {"type": "select", "options": ["false", "true"], "default": "false"}
        }
    ),
    "filter_missing": FilterMissingUnit(
        id="filter_missing",
        label="FilterSeq: missing",
        requires=["R1"],
        group="bulk",
        params_schema={
            "max_missing": {"type": "int", "default": 10, "min": 0},
            "inner": {"type": "select", "options": ["false", "true"], "default": "false"}
        }
    ),
    "filter_repeats": FilterRepeatsUnit(
        id="filter_repeats",
        label="FilterSeq: repeats",
        requires=["R1"],
        group="bulk",
        params_schema={
            "max_repeat": {"type": "text", "default": "0.8"},
            "missing": {"type": "select", "options": ["false", "true"], "default": "false"},
            "inner": {"type": "select", "options": ["false", "true"], "default": "false"}
        }
    ),
    "filter_trimqual": FilterTrimQualUnit(
        id="filter_trimqual",
        label="FilterSeq: trimqual",
        requires=["R1"],
        group="bulk",
        params_schema={
            "qmin": {"type": "int", "default": 20, "min": 0, "max": 40},
            "window": {"type": "int", "default": 10, "min": 1},
            "reverse": {"type": "select", "options": ["false", "true"], "default": "false"}
        }
    ),
    "filter_maskqual": FilterMaskQualUnit(
        id="filter_maskqual",
        label="FilterSeq: maskqual",
        requires=["R1"],
        group="bulk",
        params_schema={"qmin": {"type": "int", "default": 20, "min": 0, "max": 40}}
    ),
    "mask_primers": MaskPrimersUnit(
        id="mask_primers",
        label="MaskPrimers",
        requires=["R1"],
        group="bulk",
        params_schema={
            "variant": {"type": "select", "options": ["align", "score", "extract"], "default": "align"},
            "mode": {"type": "select", "options": ["cut", "mask", "trim", "tag"], "default": "mask"},
            "v_primers_fname": {"type": "file", "accept": ".fa,.fasta", "help": "Optional if V primers uploaded in section 1"},
            "c_primers_fname": {"type": "file", "accept": ".fa,.fasta", "optional": True, "help": "Optional if C primers uploaded"},
            "start": {"type": "int", "default": 0, "min": 0},
            "length": {"type": "int", "default": 30, "min": 1},
            "revpr": {"type": "select", "options": ["false", "true"], "default": "false"},
        }
    ),
    "collapse_seq": CollapseSeqUnit(
        id="collapse_seq",
        label="CollapseSeq (deduplicate)",
        requires=[],
        group="bulk",
        params_schema={
            "outname": {"type": "text", "default": "COLLAPSE"},
            "act": {"type": "select", "options": ["", "min", "max", "sum", "set", "majority"], "default": ""}
        }
    ),
    "build_consensus": BuildConsensusUnit(
        id="build_consensus",
        label="BuildConsensus",
        requires=[],
        group="bulk",
        params_schema={
            "qmin": {"type": "text", "placeholder": "min quality"},
            "freq": {"type": "text", "placeholder": "min freq"},
            "maxgap": {"type": "text", "placeholder": "0..1"},
            "act": {"type": "text", "placeholder": "min,max,sum,set,majority (comma sep)"},
            "dep": {"type": "select", "options": ["false", "true"], "default": "false"},
            "maxdiv": {"type": "text", "placeholder": "e.g. 0.05"},
            "maxerror": {"type": "text", "placeholder": "e.g. 0.05"},
        }
    ),
    "sc_merge_samples": MergeSamplesUnit(
        id="sc_merge_samples",
        label="SC: Merge samples (AIRR TSV)",
        requires=[],
        group="sc",
        params_schema={
            "files": {"type": "text", "placeholder": "sample1.tsv, sample2.tsv (leave empty = all *.tsv in session)"},
            "aux_types": {"type": "text", "placeholder": "v_germline_length=i, d_germline_length=i, j_germline_length=i, day=i"},
            "sample_field": {"type": "text", "default": "sample_id", "help": "Annotate each row with filename stem; empty to skip"}
        },
    ),
    "sc_filter_productive": SC_FilterProductiveUnit(
        id="sc_filter_productive",
        label="SC: Keep productive sequences (independent)",
        requires=[],
        group="sc",
        params_schema={
            "files": {"type": "text", "placeholder": "file1.tsv file2.tsv (blank = all *.tsv/*.tsv.gz)"},
            "productive_field": {"type": "text", "default": "productive", "help": "Column with TRUE/T/1"},
            "fallback_from_airr": {"type": "select", "options": ["true", "false"], "default": "true",
                                  "help": "If 'productive' missing, use (vj_in_frame & !stop_codon)"},
            "mode": {"type": "select", "options": ["merge", "per_file"], "default": "merge"},
            "sample_field": {"type": "text", "default": "sample_id", "help": "Add origin column when merging"}
        },
    ),
    "sc_remove_multi_heavy": SC_RemoveMultiHeavyUnit(
        id="sc_remove_multi_heavy",
        label="SC: Remove cells with multiple heavy chains (independent)",
        requires=[],
        group="sc",
        params_schema={
            "files": {"type": "text", "placeholder": "file1.tsv file2.tsv (blank = all *.tsv/*.tsv.gz)"},
            "locus_field": {"type": "text", "default": "locus", "help": "Column with chain locus (IGH/IGK/IGL)"},
            "heavy_value": {"type": "text", "default": "IGH", "help": "Value indicating heavy locus"},
            "cell_field": {"type": "text", "default": "cell_id", "help": "Cell identifier column (required)"},
            "fallback_from_vcall": {"type": "select", "options": ["true", "false"], "default": "true",
                                    "help": "If locus missing, detect heavy via v_call =~ '^IGH'"},
            "mode": {"type": "select", "options": ["merge", "per_file"], "default": "merge"},
            "sample_field": {"type": "text", "default": "sample_id", "help": "Add origin column when merging"}
        },
    ),
    "sc_remove_no_heavy": SC_RemoveNoHeavyUnit(
        id="sc_remove_no_heavy",
        label="SC: Remove cells without heavy chains (independent)",
        requires=[],
        group="sc",
        params_schema={
            "files": {"type": "text", "placeholder": "file1.tsv file2.tsv (blank = all *.tsv/*.tsv.gz)"},
            "locus_field": {"type": "text", "default": "locus", "help": "Column indicating locus (IGH/IGK/IGL)"},
            "heavy_value": {"type": "text", "default": "IGH", "help": "Value for heavy locus"},
            "light_values": {"type": "text", "default": "IGK, IGL", "help": "Values for light loci"},
            "cell_field": {"type": "text", "default": "cell_id", "help": "Cell identifier column"},
            "fallback_from_vcall": {"type": "select", "options": ["true", "false"], "default": "true",
                                    "help": "If locus missing, infer heavy/light from v_call"},
            "mode": {"type": "select", "options": ["merge", "per_file"], "default": "merge"},
            "sample_field": {"type": "text", "default": "sample_id", "help": "Add origin column when merging"}
        },
    ),
}

__all__ = ["UnitSpec", "UNITS"]

