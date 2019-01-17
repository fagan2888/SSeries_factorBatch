# SSeries StyleName Definition

- Factor Definitions used in SSeries Model
- Research by Eunjae Jang



## Accruals

> (Net_Operating_Cashflow - Net_Income_befExtraordinaryItems) / Total_Asset

$$ Accruals_{fq} = \frac{CF_{fq} - NI_{fq}}{TA_{fq}} $$

$$ Accruals_{fy} = \frac{CF_{fy} - NI_{fy}}{TA_{fy}} $$



## Accruals.S

>(Net_Operating_Cashflow - Net_Income_befExtraordinaryItems) / Revenue

$$ Accruals.S_{fq} = \frac{CF_{fq} - NI_{fq}}{RV_{fq}} $$

$$ Accruals.S_{fy} = \frac{CF_{fy} - NI_{fy}}{RV_{fy}} $$



## ARDays (Accounts Receivables Days)

> -365 * (Accounts Receivables / Revenue)

$$ ARDays_{fq} = -365  \frac{AR_{fq}}{RV_{fq}} $$

$$ ARDays_{fy} = -365  \frac{AR_{fy}}{RV_{fy}} $$



## CAcqR (Capital Aquisition Ratio)

> (Net_Operating_Cashflow + Cash_Dividend) / Capex

$$ CAcqR_{fq} = \frac{CF_{fq} + DVD_{fq}}{Capex_{fq}} $$

$$ CAcqR_{fy} = \frac{CF_{fy} + DVD_{fy}}{Capex_{fy}} $$



## EarnRev (Earnings Revision 3M)

> $EstEPS_{t, fy1} - EstEPS_{t-3M, fy1}$ / abs($EstEPS_{t-3M, fy1}$) 

$$ EarnRev_t = \frac{(EstEPS_{t, fy1} - EstEPS_{t-3M, fy1})}{| EstEPS_{t-3M, fy1} |} $$



## GARP Related:

### ROE_l2yr

> Last 2year Average Historical ROE (qtr & year)



### Payout_l2yr

> Last 2year Average Historical Payout (qtr & year)



### EPSg_l2yr

> Last 2year Average of EPS Growth (qtr & year)



### SustG (Sustainable Growth)

> Normalized_ROE * (1 - Normalized_Payout)

where, $ ROE_{norm} = (ROE_{l2yr} + ROE_{f2yr}) / 2 $

and, $ Payout_{norm} = (Payout_{l2yr} + Payout_{f2yr}) / 2 $

$$ SustG = ROE_{norm} \times (1 - Payout_{norm}) $$



### PE_f12m (Forward PE 12M)

> Forward PE 12M  (weighted average of FY1 & FY2 EPS)

$$ PE_{f12m} = \frac{MktCap}{EPS_{fy1}} $$



### DY_f12m (Forward DividendYield 12M)

> Forward DividendYield 12M  (weighted average of FY1 & FY2 DPS)

$$ DY_{f12m} = \frac{DVD_{fy1}}{MktCap} $$



### EPSg_f2yr (Exp. Future 2yr EPS growth Average)

> IBES EPS fy1/fy0, fy2/fy1 average

$$ EPSg_{f2yr} = (\frac{EPS_{fy2} - EPS_{fy1}}{EPS_{fy1}} + \frac{EPS_{fy1} - EPS_{fy0}}{EPS_{fy0}}) / 2 $$



### ROEavg_f2yr (Exp. Future 2yr ROE Average)

> IBES ROE fy0, fy1, fy2 average

$$ ROE_{f2yr} = (ROE_{fy2} + ROE_{fy2} + ROE_{fy2}) / 3 $$



### EGP

> 1 / PEG = EPSg_f2yr / PE_f12m

$$ EGP = EGPg_{f2yr} / PE_{f12m} $$



### SGP

> Sustainable_Growth / PE_f12m

$$ SGP = SustG / PE_{f12m} $$



## EVEbtida (EV2EBITDA)

> EnterpriseValue / (Operating_Income + Depreciation&Amortization)

$$ EVEbitda_{fq} = \frac{EV_{fq}}{OI_{fq} + DA_{fq}} $$

$$ EVEbitda_{fy} = \frac{EV_{fy}}{OI_{fy} + DA_{fy}} $$



## CFO2EV

> Net_Operating_Cashflow / EnterpriseValue

$$ CFO2EV_{fq} = \frac{CF_{fq}}{EV_{fq}} $$

$$ CFO2EV_{fy} = \frac{CF_{fy}}{EV_{fy}} $$



## FCF2EV

> Free-CashFlow / Enterprise Value

$$ FCF2EV_{fq} = \frac{CF_{fq} - Capex_{fq}}{EV_{fq}} $$

$$ FCF2EV_{fy} = \frac{CF_{fy} - Capex_{fy}}{EV_{fy}} $$



## CFROIC

> Net_Operating_Cashflow / (Long-term_Debt + Total_Equity + Minority_Interest)

$$ CFROIC_{fq} = \frac{CF_{fq}}{LtB_{fq} + Eq_{fq} + MI_{fq}} $$

$$ CFROIC_{fy} = \frac{CF_{fy}}{LtB_{fy} + Eq_{fy} + MI_{fy}} $$



## FCFROIC

> (Net_Operating_Cashflow - Capex) / (Long-term_Debt + Total_Equity + Minority_Interest)

$$ FCFROIC_{fq} = \frac{CF_{fq} - Capex_{fq}}{LtB_{fq} + Eq_{fq} + MI_{fq}} $$

$$ FCFROIC_{fy} = \frac{CF_{fy} - Capex_{fy}}{LtB_{fy} + Eq_{fy} + MI_{fy}} $$



## ROIC

> (Operating_Income * Effective_Tax_Rate) / (Total_Asset - (Short-Term_Cash + PPE + InvestAssociatedCompanies))

$$ ROIC_{fq} = \frac{OI_{fq} \times Tax_{fq}}{TA_{fq} -(Csh_{fq} + PPE_{fq} + AC_{fq})} $$

$$ ROIC_{fy} = \frac{OI_{fy} \times Tax_{fy}}{TA_{fy} -(Csh_{fy} + PPE_{fy} + AC_{fy})} $$



## 5Yr Relative Valuation

### 5YRel_CFO2EV

> Z-Score of Current to Past 5yr CFO2EV

$$ 5YRel\_CFO2EV_{fq} = \frac{CFO2EV_{fq} - AVG_{i=t-20Q}^t (CFO2EV_{fq})}{STD_{i=t-20Q}^t (CFO2EV_{fq})} $$

$$ 5YRel\_CFO2EV_{fy} = \frac{CFO2EV_{fy} - AVG_{i=t-5Y}^t (CFO2EV_{fy})}{STD_{i=t-5Y}^t (CFO2EV_{fy})} $$



### 5YRel_CFO2P

> Z-Score of Current to Past 5yr CFO2P

$$ 5YRel\_CFO2P_{fq} = \frac{CFO2P_{fq} - AVG_{i=t-20Q}^t (CFO2P_{fq})}{STD_{i=t-20Q}^t (CFO2P_{fq})} $$

$$ 5YRel\_CFO2P_{fy} = \frac{CFO2P_{fy} - AVG_{i=t-5Y}^t (CFO2P_{fy})}{STD_{i=t-5Y}^t (CFO2P_{fy})} $$



### 5YRel_EBITDA2P

> Z-Score of Current to Past 5yr EBITDA2P

$$ 5YRel\_EBITDA2P_{fq} = \frac{EBITDA2P_{fq} - AVG_{i=t-20Q}^t (EBITDA2P_{fq})}{STD_{i=t-20Q}^t (EBITDA2P_{fq})} $$

$$ 5YRel\_EBITDA2P_{fy} = \frac{EBITDA2P_{fy} - AVG_{i=t-5Y}^t (EBITDA2P_{fy})}{STD_{i=t-5Y}^t (EBITDA2P_{fy})} $$



## GPOA

> Gross_Income / Total_Asset

$$ GPOA_{fq} = \frac{GI_{fq}}{TA_{fq}} $$

$$ GPOA_{fy} = \frac{GI_{fy}}{TA_{fy}} $$



## IntC (Interest Coverage Ratio)

> Operating_Income / Interest_Expense

$$ IntC_{fq} = \frac{OI_{fq}}{IE_{fq}} $$

$$ IntC_{fy} = \frac{OI_{fy}}{IE_{fy}} $$



## LTDtE (Long-Term Debt Ratio)

> Long-term_Debt / Total_Equity

$$ LTDtE_{fq} = \frac{LtD_{fq}}{Eq_{fq}} $$

$$ LTDtE_{fy} = \frac{LtD_{fy}}{Eq_{fy}} $$



## NM (Net-Margin)

> Trivial



## OPM (Operating-Profit Margin)

> Trivial



## EBITDAM (Ebitda Margin)

> Trivial



## NM_l2yrAvg_chg

> 2Year Average of NM_chg(YoY)

$$ NMchg_{2yrAvg, fq} = \frac{(\sum_{i=t-4Q}^t NM_{i} - NM_{i-4Q})}{4} $$

$$ NMchg_{2yrAvg, fy} = \frac{(\sum_{i=t-1yr}^t NM_{i} - NM_{i-1yr})}{2} $$



## OPM_l2yrAvg_chg

> 2Year Average of OPM_chg(YoY)

$$ OPMchg_{2yrAvg, fq} = \frac{(\sum_{i=t-4Q}^t OPM_{i} - OPM_{i-4Q})}{4} $$

$$ OPMchg_{2yrAvg, fy} = \frac{(\sum_{i=t-1yr}^t OPM_{i} - OPM_{i-1yr})}{2} $$



## EBITDAM_l2yrAvg_chg

> 2Year Average of EbitdaM_chg(YoY)

$$ EbitdaMchg_{2yrAvg, fq} = \frac{(\sum_{i=t-4Q}^t EbitdaM_{i} - EbitdaM_{i-4Q})}{4} $$

$$ EbitdaMchg_{2yrAvg, fy} = \frac{(\sum_{i=t-1yr}^t EbitdaM_{i} - EbitdaM_{i-1yr})}{2} $$



## OpLev (Operating Leverage)

> (Revenue - COGS_exDepreciation) / Operating_Income

$$ OpLev_{fq} = \frac{RV_{fq} - COGS_{fq}}{OP_{fq}} $$

$$ OpLev_{fy} = \frac{RV_{fy} - COGS_{fy}}{OP_{fy}} $$



## ROE_ws

> Trivial



## ROA_ws

> Trivial



## GPM_ws

> Trivial (Gross-profit margin)



## REVg_l1yr_ws

> Trivial (last 1year Revenue Growth)



## NIg_l1yr_ws

> Trivial (last 1year Net-Income Growth)

