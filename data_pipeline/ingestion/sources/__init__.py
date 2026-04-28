# Source connectors package.

from data_pipeline.ingestion.sources.acs5 import ACS5Connector
from data_pipeline.ingestion.sources.bds import BDSConnector
from data_pipeline.ingestion.sources.bea_gdp import BEAGDPConnector
from data_pipeline.ingestion.sources.bea_rpp import BEARPPConnector
from data_pipeline.ingestion.sources.cbp_zbp import CBPZBPConnector
from data_pipeline.ingestion.sources.college_scorecard import CollegeScorecardConnector
from data_pipeline.ingestion.sources.fcc_broadband import FCCBroadbandConnector
from data_pipeline.ingestion.sources.ipeds import IPEDSConnector
from data_pipeline.ingestion.sources.irs_migration import IRSMigrationConnector
from data_pipeline.ingestion.sources.laus import LAUSConnector
from data_pipeline.ingestion.sources.lehd_lodes import LEHDLODESConnector
from data_pipeline.ingestion.sources.oews import OEWSConnector
from data_pipeline.ingestion.sources.onet import ONETConnector
from data_pipeline.ingestion.sources.pop_estimates import PopulationEstimatesConnector
from data_pipeline.ingestion.sources.qcew import QCEWConnector
from data_pipeline.ingestion.sources.ruca_rucc import RUCARUCCConnector
from data_pipeline.ingestion.sources.abs_australia import ABSAustraliaConnector
from data_pipeline.ingestion.sources.eurostat import EurostatConnector
from data_pipeline.ingestion.sources.india_worldbank import IndiaWorldBankConnector

PHASE2_CORE_CONNECTORS = [
    OEWSConnector,
    LAUSConnector,
    QCEWConnector,
    ACS5Connector,
    PopulationEstimatesConnector,
    CBPZBPConnector,
    BEARPPConnector,
    BEAGDPConnector,
    BDSConnector,
]

PHASE3_EXPANSION_CONNECTORS = [
    LEHDLODESConnector,
    IRSMigrationConnector,
    IPEDSConnector,
    CollegeScorecardConnector,
    ONETConnector,
    RUCARUCCConnector,
    FCCBroadbandConnector,
]

GLOBAL_CONNECTORS = [
    ABSAustraliaConnector,
    EurostatConnector,
    IndiaWorldBankConnector,
]

ALL_CONNECTORS = PHASE2_CORE_CONNECTORS + PHASE3_EXPANSION_CONNECTORS + GLOBAL_CONNECTORS
