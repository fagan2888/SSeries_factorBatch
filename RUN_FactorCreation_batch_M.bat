call activate batchenv
python.exe "initiate_batch.py" "batch" "M"

python.exe "make_0a_ibes_roe.py" "batch" "M"
python.exe "make_1a_ibes_edp.py" "batch" "M"
python.exe "make_2b_SustG.py" "batch" "M"
python.exe "make_3b_GARP.py" "batch" "M"
python.exe "make_5yrRel_CFO2EV.py" "batch" "M"
python.exe "make_5yrRel_CFO2px.py" "batch" "M"
python.exe "make_5yrRel_EBITDA2px.py" "batch" "M"
python.exe "make_Accruals.py" "batch" "M"
python.exe "make_ARDays.py" "batch" "M"
python.exe "make_AxiomaMomentum.py" "batch" "M"
python.exe "make_CAcqR.py" "batch" "M"
python.exe "make_EarnRev.py" "batch" "M"
python.exe "make_EVEbitda.py" "batch" "M"
python.exe "make_FCFROIC.py" "batch" "M"
python.exe "make_GPOA.py" "batch" "M"
python.exe "make_IntC.py" "batch" "M"
python.exe "make_LTDebt.py" "batch" "M"
python.exe "make_margin.py" "batch" "M"
python.exe "make_OpLev.py" "batch" "M"
python.exe "make_Ratios.py" "batch" "M"
python.exe "make_ROIC.py" "batch" "M"
python.exe "make_Starmine.py" "batch" "M"
python.exe "make_Value.py" "batch" "M"

python.exe "Upload_Factors_M.py" "batch"
call deactivate