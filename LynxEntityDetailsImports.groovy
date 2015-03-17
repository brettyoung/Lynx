import com.navis.argo.ContextHelper
import com.navis.argo.business.api.GroovyApi
import com.navis.argo.business.api.ServicesManager
import com.navis.argo.business.atoms.*
import com.navis.argo.business.model.CarrierVisit
import com.navis.argo.business.model.Complex
import com.navis.argo.business.model.Facility
import com.navis.argo.business.reference.Equipment
import com.navis.argo.business.reference.ScopedBizUnit
import com.navis.cargo.business.model.BillOfLading
import com.navis.cargo.business.model.BlGoodsBl
import com.navis.cargo.business.model.GoodsBl
import com.navis.framework.business.Roastery
import com.navis.framework.metafields.MetafieldId
import com.navis.framework.metafields.MetafieldIdFactory
import com.navis.framework.persistence.HibernateApi
import com.navis.framework.portal.FieldChanges
import com.navis.framework.portal.query.DomainQuery
import com.navis.framework.portal.query.PredicateFactory
import com.navis.framework.portal.query.QueryFactory
import com.navis.inventory.InventoryEntity
import com.navis.inventory.InventoryField
import com.navis.inventory.business.api.UnitField
import com.navis.inventory.business.api.UnitFinder
import com.navis.inventory.business.units.GoodsBase
import com.navis.inventory.business.units.Unit
import com.navis.inventory.business.units.UnitFacilityVisit
import com.navis.services.business.event.Event
import com.navis.services.business.rules.ServiceImpediment
import com.navis.vessel.VesselEntity
import com.navis.vessel.api.VesselVisitField
import com.navis.vessel.business.schedule.VesselVisitDetails

import java.text.SimpleDateFormat

class LynxEntityDetailsImports extends GroovyApi {
    public String execute(Map inParameters) {
        Facility facility = ContextHelper.getThreadFacility()
        Complex complex = ContextHelper.getThreadComplex()
        String gKeyParam = inParameters.get("Gkey")
        String billNbrParam = inParameters.get("Bills of Lading")
        String lineParam = inParameters.get("Line")
        String vesselParam = inParameters.get("Vessel")
        String voyageParam = inParameters.get("Voyage")
        String eqNbrParam = inParameters.get("Container")
        String drayStatusParam = inParameters.get("Dray Status")
        String drayTruckParam = inParameters.get("Dray Truck")
        String gvyClassParam = inParameters.get("Groovy Class")


        if (gvyClassParam.toUpperCase().equals("IMPORTINQUIRYBYCONTAINER")) {
            StringBuilder results = new StringBuilder()
            String[] equipArr = eqNbrParam.toUpperCase().split(",")
            for (String equip : equipArr) {
                String[] lineArr = lineParam.toUpperCase().split(",")
                for (String line : lineArr) {
                    Equipment equipment = Equipment.findEquipment(equip)
                    if (equipment == null) {
                        if (equipArr.size().equals(1)) {
                        return "Error:  Container not found."
                        } else {
                            continue
                        }
                    }
                    UnitFinder unitFinder = (UnitFinder) Roastery.getBean("unitFinder")
                    Unit unit = unitFinder.findActiveUnit(complex, equipment)
                    if (unit.getUnitCategory() != UnitCategoryEnum.IMPORT) {
                        unit = unitFinder.findDepartedUnit(complex, equipment)
                    }
                    if (unit == null) {
                        return "Error:  Container not found."
                    }
                    if (!unit.getUnitLineOperator().getBzuId().equalsIgnoreCase(line)) {
                        continue
                    }
                    GoodsBase goodsBase = unit.getUnitGoods()
                    GoodsBl goodsBl = GoodsBl.resolveGoodsBlFromGoodsBase(goodsBase)
                    Set<BillOfLading> billOfLadingSet
                    String bls = ""
                    if (goodsBl != null) {
                        billOfLadingSet = goodsBl.getGdsblBillsOfLading()
                        for (BillOfLading blNbr : billOfLadingSet) {
                            bls = bls + blNbr.getBlNbr() + ","
                        }
                    }
                    List<String> billHolds = new ArrayList<String>()
                    String freight
                    String customs
                    String holds
                    if (goodsBl != null) {
                        for (BillOfLading bl : billOfLadingSet) {
                            ServicesManager serviceManager = (ServicesManager) Roastery.getBean(ServicesManager.BEAN_ID);
                            if (serviceManager.getImpedimentsForEntity(bl) != null) {
                                Set<ServiceImpediment> impedSet = serviceManager.getImpedimentsForEntity(bl)
                                for (ServiceImpediment servImped : impedSet) {
                                    if (servImped.getStatus().equals(FlagStatusEnum.ACTIVE)) {
                                        billHolds.add(servImped.toString())
                                    }
                                }
                            }
                            holds = billHolds.toString().replace("[", "").replace("]", "")
                            if (billHolds == null) {
                                freight = "RELEASED"
                                customs = "RELEASED"
                            } else {
                                if (holds.toUpperCase().contains("FREIGHT")) {
                                    freight = "HOLD"
                                } else {
                                    freight = "RELEASED"
                                }
                                if (holds.toUpperCase().contains("CUSTOMS")) {
                                    customs = "HOLD"
                                } else {
                                    customs = "RELEASED"
                                }
                            }
                        }
                    }
                    String readyFl
                    if (unit.getUnitStoppedRoad()) {
                        readyFl = "NO"
                    } else {
                        if (unit.getLocType().equals(LocTypeEnum.YARD)) {
                            readyFl = "YES"
                        } else {
                            readyFl = "NO"
                        }
                    }
                    UnitFacilityVisit ufv = unit.getUfvForEventTime(unit.getUnitTimeLastStateChange())// .getUnitActiveUfvNowActive()
                    Date lfd = ufv.getUfvCalculatedLastFreeDayDate()
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                    String formatLfd
                    if (lfd == null) {
                        formatLfd = ""
                    } else {
                        formatLfd = sdf.format(lfd)
                    }
                    String goodThru
                    if (ufv.getUfvCalculatedLastFreeDayDate() > ufv.getUfvPaidThruDay()) {
                        goodThru = formatLfd
                    } else {
                        goodThru = ufv.getUfvPaidThruDay().toString()
                    }
                    String trucker = (unit.getUnitRtgTruckingCompany() == null) ? "" : unit.getUnitRtgTruckingCompany().getBzuId() + " - " + unit.getUnitRtgTruckingCompany().getBzuName()
                    MetafieldId aqi = MetafieldIdFactory.valueOf("customFlexFields.ufvCustomDFFAQIAmt")
                    MetafieldId dem = MetafieldIdFactory.valueOf("customFlexFields.ufvCustomDFFDemurrageAmt")
                    MetafieldId vacis = MetafieldIdFactory.valueOf("customFlexFields.ufvCustomDFFVacisAmt")
                    MetafieldId other = MetafieldIdFactory.valueOf("customFlexFields.ufvCustomDFFOtherAmt")
                    Double aqiAmt = (ufv.getField(aqi) == null) ? 0 : ufv.getField(aqi)
                    Double demAmt = (ufv.getField(dem) == null) ? 0 : ufv.getField(dem)
                    Double vacisAmt = (ufv.getField(vacis) == null) ? 0 : ufv.getField(vacis)
                    Double otherAmt = (ufv.getField(other) == null) ? 0 : ufv.getField(other)
                    Double totalCharges = aqiAmt + demAmt + vacisAmt + otherAmt
                    bls = (bls == "") ? "" : bls.substring(0, bls.length() - 1)

                    results.append("<TABLE>")
                            .append("<GKEY>").append(unit.getUnitGkey()).append("</GKEY>")
                            .append("<EQUIPMENTNBR>").append(unit.getUnitId()).append("</EQUIPMENTNBR>")
                            .append("<BILLOFLADING>").append(bls).append("</BILLOFLADING>")
                            .append("<READYFORDELIVERY>").append(readyFl).append("</READYFORDELIVERY>")
                            .append("<FREIGHT>").append(freight).append("</FREIGHT>")
                            .append("<CUSTOMS>").append(customs).append("</CUSTOMS>")
                            .append("<HOLDS>").append(unit.getUnitImpediments()).append("</HOLDS>")
                            .append("<ROADIMPEDIMENTS>").append(unit.getUnitImpedimentRoad()).append("</ROADIMPEDIMENTS>")
                            .append("<GOODTHRU>").append(goodThru).append("</GOODTHRU>")
                            .append("<PORT_PTD>").append(ufv.getUfvPaidThruDay()).append("</PORT_PTD>")
                            .append("<PORT_LFD>").append(formatLfd).append("</PORT_LFD>")
                            .append("<DEMURRAGE>").append("</DEMURRAGE>")
                            .append("<LOCATIONDETAILS>").append(ufv.getUfvLastKnownPosition().toString()).append("</LOCATIONDETAILS>")
                            .append("<LOCATION>").append(ufv.getUfvLastKnownPosition().getPosLocType().getKey()).append("</LOCATION>")
                            .append("<SHIPPINGLINE>").append(unit.getUnitLineOperator().getBzuId()).append("</SHIPPINGLINE>")
                            .append("<SHIPPINGLINEDETAILS>").append(unit.getUnitLineOperator().getBzuName()).append("</SHIPPINGLINEDETAILS>")
                            .append("<DISCHARGED>").append(ufv.getUfvTimeIn()).append("</DISCHARGED>")
                            .append("<EQSZ>").append(equipment.getEqEquipType().getEqtypNominalLength().getKey()).append("</EQSZ>")
                            .append("<EQTP>").append(equipment.getEqEquipType().getEqtypIsoGroup().getKey()).append("</EQTP>")
                            .append("<EQHT>").append(equipment.getEqEquipType().getEqtypNominalHeight().getKey()).append("</EQHT>")
                            .append("<GROSSWEIGHT>").append(unit.getUnitGoodsAndCtrWtKg()).append("</GROSSWEIGHT>")
                            .append("<CATEGORY>").append(unit.getUnitCategory().getKey()).append("</CATEGORY>")
                            .append("<HAZ_CLASS>").append(unit.getUnitWorstHazardClass()).append("</HAZ_CLASS>")
                            .append("<CATEGORYDETAILS>").append(unit.getUnitCategory().getKey()).append("</CATEGORYDETAILS>")
                            .append("<STATUS>").append(unit.getUnitFreightKind().getKey()).append("</STATUS>")
                            .append("<STATUSDETAILS>").append(unit.getUnitFreightKind().getKey()).append("</STATUSDETAILS>")
                            .append("<OOGFL>").append(unit.getUnitIsOog()).append("</OOGFL>")
                            .append("<OUT_TIME>").append(ufv.getUfvTimeOut()).append("</OUT_TIME>")
                            .append("<VESSELVOYAGE>").append(ufv.getInboundCarrierVisit().getCvId()).append("</VESSELVOYAGE>")
                            .append("<VESSEL>").append(ufv.getInboundCarrierVisit().getCvCvd().getCarrierVehicleId()).append("</VESSEL>")
                            .append("<VOYAGE>").append(ufv.getInboundCarrierVisit().getCvCvd().getCarrierIbVoyNbrOrTrainId()).append("</VOYAGE>")
                            .append("<TRUCKER>").append(trucker).append("</TRUCKER>")
                            .append("<TOTALCHARGES>").append(totalCharges).append("</TOTALCHARGES>")
                            .append("<GTD>").append(ufv.getUfvGuaranteeThruDay()).append("</GTD>")
                            .append("</TABLE>")
                }
            }
            return results.toString().replace("null", "").replace("NOM", "")
        } else if (gvyClassParam.toUpperCase().equals("BILLOFLADINGHEADER")) {
            StringBuilder results = new StringBuilder()

            String[] billArr = billNbrParam.toUpperCase().split(",")
            for (String bill : billArr) {
                String[] lineArr = lineParam.toUpperCase().split(",")
                for (String line : lineArr) {
                    List bills = BillOfLading.findAllBillsOfLading(bill)
                    BillOfLading billOfLading = bills.get(0)
                    if (!billOfLading.getBlLineOperator().getBzuId().equalsIgnoreCase(line)) {
                        continue
                    }
                    List<String> billHolds = new ArrayList<String>()
                    ServicesManager serviceManager = (ServicesManager) Roastery.getBean(ServicesManager.BEAN_ID);
                    if (serviceManager.getImpedimentsForEntity(billOfLading) != null) {
                        Set<ServiceImpediment> impedSet = serviceManager.getImpedimentsForEntity(billOfLading)
                        for (ServiceImpediment servImped : impedSet) {
                            if (servImped.getStatus().equals(FlagStatusEnum.ACTIVE)) {
                                billHolds.add(servImped.toString())
                            }
                        }
                    }
                    String freight
                    String customs
                    String holds = billHolds.toString().replace("[", "").replace("]", "")
                    if (billHolds == null) {
                        freight = "RELEASED"
                        customs = "RELEASED"
                    } else {
                        if (holds.toUpperCase().contains("FREIGHT")) {
                            freight = "HOLD"
                        } else {
                            freight = "RELEASED"
                        }
                        if (holds.toUpperCase().contains("CUSTOMS")) {
                            customs = "HOLD"
                        } else {
                            customs = "RELEASED"
                        }
                    }
                    InbondEnum inbondEnum = billOfLading.getInbondStatus()
                    String inbondStatus = (inbondEnum == null) ? "" : inbondEnum.getKey()
                    ExamEnum examEnum = billOfLading.getExamStatus()
                    String examStatus = (examEnum == null) ? "" : examEnum.getKey()
                    results.append("<TABLE>")
                            .append("<BILLOFLADING>").append(billOfLading.getBlNbr()).append("</BILLOFLADING>")
                            .append("<GKEY>").append(billOfLading.getBlGkey()).append("</GKEY>")
                            .append("<SHIPPINGLINE>").append(billOfLading.getBlLineOperator().getBzuId()).append("</SHIPPINGLINE>")
                            .append("<SHIPPINGLINEDETAILS>").append(billOfLading.getBlLineOperator().getBzuName()).append("</SHIPPINGLINEDETAILS>")
                            .append("<CUSTOMS>").append(customs).append("</CUSTOMS>")
                            .append("<FREIGHT>").append(freight).append("</FREIGHT>")
                            .append("<HOLDS>").append(holds).append("</HOLDS>")
                            .append("<VESSEL>").append(billOfLading.getBlCarrierVisit().getCarrierVehicleId()).append("</VESSEL>")
                            .append("<VESSELNAME>").append(billOfLading.getBlCarrierVisit().getCarrierVehicleName()).append("</VESSELNAME>")
                            .append("<VOYAGE>").append(billOfLading.getBlCarrierVisit().getCarrierIbVoyNbrOrTrainId()).append("</VOYAGE>")
                            .append("<NOTES>").append(billOfLading.getBlNotes()).append("</NOTES>")
                            .append("<PIECECOUNT>").append(billOfLading.getBlManifestedQty()).append("</PIECECOUNT>")
                            .append("<SHIPPER>").append(billOfLading.getShipperName()).append("</SHIPPER>")
                            .append("<DESTINATION>").append(billOfLading.getBlDestination()).append("</DESTINATION>")
                            .append("<ORIGIN>").append(billOfLading.getBlOrigin()).append("</ORIGIN>")
                            .append("<INBOND>").append(inbondStatus).append("</INBOND>")
                            .append("<EXAM>").append(examStatus).append("</EXAM>")
                            .append("<MANIFESTED>").append(billOfLading.getBlManifestedQty()).append("</MANIFESTED>")
                            .append("<RELEASED>").append(billOfLading.getBlReleasedQty()).append("</RELEASED>")
                            .append("<ENTERED>").append(billOfLading.getBlEnteredQty()).append("</ENTERED>")
                            .append("<POD1>").append(billOfLading.getPod1Id()).append("</POD1>")
                            .append("<CONSIGNEE>").append(billOfLading.getBlConsigneeAsString()).append("</CONSIGNEE>")
                            .append("<POL>").append(billOfLading.getPolId()).append("</POL>")
                            .append("</TABLE>")
                }
            }
            return results.toString().replace("null", "").replace("NOM", "")
        } else if (gvyClassParam.toUpperCase().equals("IMPORTINQUIRYBYBL")) {
            StringBuilder results = new StringBuilder()

            String[] billArr = billNbrParam.toUpperCase().split(",")
            for (String bill : billArr) {
                String[] lineArr = lineParam.toUpperCase().split(",")
                for (String line : lineArr) {
                    List bills = BillOfLading.findAllBillsOfLading(bill)
                    if (bills.isEmpty()) {
                        if(billArr.size().equals(1)) {
                            return "Error:  Bill of Lading not found"
                        } else {
                            continue
                        }
                    }
                    BillOfLading billOfLading = bills.get(0)
                    if (!billOfLading.getBlLineOperator().getBzuId().equalsIgnoreCase(line)) {
                        continue
                    }
                    List<String> billHolds = new ArrayList<String>()
                    ServicesManager serviceManager = (ServicesManager) Roastery.getBean(ServicesManager.BEAN_ID);
                    if (serviceManager.getImpedimentsForEntity(billOfLading) != null) {
                        Set<ServiceImpediment> impedSet = serviceManager.getImpedimentsForEntity(billOfLading)
                        for (ServiceImpediment servImped : impedSet) {
                            if (servImped.getStatus().equals(FlagStatusEnum.ACTIVE)) {
                                billHolds.add(servImped.toString())
                            }
                        }
                    }
                    Set<BlGoodsBl> blGoodsBlSet = billOfLading.getBlBlGoodsBls()
                    for (BlGoodsBl blGoodsBl : blGoodsBlSet) {
                        GoodsBl goodsBl = blGoodsBl.getBlgdsblGoodsBl()
                        Unit unit = goodsBl.getGdsUnit()
                        Equipment equipment = unit.getPrimaryEq()
                        UnitFacilityVisit ufv = unit.getUfvForEventTime(blGoodsBl.getBlgdsblCreated()) //.getUnitActiveUfvNowActive()
                        GoodsBase goodsBase = unit.getUnitGoods()
                        GoodsBl goodsBls = GoodsBl.resolveGoodsBlFromGoodsBase(goodsBase)
                        Set<BillOfLading> billOfLadingSet = goodsBls.getGdsblBillsOfLading()
                        String bls = ""
                        for (BillOfLading blNbr : billOfLadingSet) {
                            bls = bls + blNbr.getBlNbr() + ","
                        }
                        String readyFl
                        if (unit.getUnitStoppedRoad()) {
                            readyFl = "NO"
                        } else {
                            if (unit.getLocType().equals(LocTypeEnum.YARD)) {
                                readyFl = "YES"
                            } else {
                                readyFl = "NO"
                            }
                        }
                        String freight
                        String customs
                        String holds = billHolds.toString().replace("[", "").replace("]", "")
                        if (billHolds == null) {
                            freight = "RELEASED"
                            customs = "RELEASED"
                        } else {
                            if (holds.toUpperCase().contains("FREIGHT")) {
                                freight = "HOLD"
                            } else {
                                freight = "RELEASED"
                            }
                            if (holds.toUpperCase().contains("CUSTOMS")) {
                                customs = "HOLD"
                            } else {
                                customs = "RELEASED"
                            }
                        }
                        Date lfd = ufv.getUfvCalculatedLastFreeDayDate()
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                        String formatLfd
                        if (lfd == null) {
                            formatLfd = ""
                        } else {
                            formatLfd = sdf.format(lfd)
                        }
                        String goodThru
                        if (ufv.getUfvCalculatedLastFreeDayDate() > ufv.getUfvPaidThruDay()) {
                            goodThru = formatLfd
                        } else {
                            goodThru = ufv.getUfvPaidThruDay().toString()
                        }
                        String trucker = (unit.getUnitRtgTruckingCompany() == null) ? "" : unit.getUnitRtgTruckingCompany().getBzuId() + " - " + unit.getUnitRtgTruckingCompany().getBzuName()
                        MetafieldId aqi = MetafieldIdFactory.valueOf("customFlexFields.ufvCustomDFFAQIAmt")
                        MetafieldId dem = MetafieldIdFactory.valueOf("customFlexFields.ufvCustomDFFDemurrageAmt")
                        MetafieldId vacis = MetafieldIdFactory.valueOf("customFlexFields.ufvCustomDFFVacisAmt")
                        MetafieldId other = MetafieldIdFactory.valueOf("customFlexFields.ufvCustomDFFOtherAmt")
                        Double aqiAmt = (ufv.getField(aqi) == null) ? 0 : ufv.getField(aqi)
                        Double demAmt = (ufv.getField(dem) == null) ? 0 : ufv.getField(dem)
                        Double vacisAmt = (ufv.getField(vacis) == null) ? 0 : ufv.getField(vacis)
                        Double otherAmt = (ufv.getField(other) == null) ? 0 : ufv.getField(other)
                        Double totalCharges = aqiAmt + demAmt + vacisAmt + otherAmt
                        try {
                            CarrierVisit cv = ufv.getInboundCarrierVisit()
                        } catch (Exception e) {
                            return "Error:  Invalid vessel associated with BL: " + bill
                        }

                        results.append("<TABLE>")
                                .append("<GKEY>").append(unit.getUnitGkey()).append("</GKEY>")
                                .append("<EQUIPMENTNBR>").append(unit.getUnitId()).append("</EQUIPMENTNBR>")
                                .append("<BILLOFLADING>").append(bls.substring(0, bls.length() - 1)).append("</BILLOFLADING>")
                                .append("<READYFORDELIVERY>").append(readyFl).append("</READYFORDELIVERY>")
                                .append("<FREIGHT>").append(freight).append("</FREIGHT>")
                                .append("<CUSTOMS>").append(customs).append("</CUSTOMS>")
                                .append("<HOLDS>").append(unit.getUnitImpediments()).append("</HOLDS>")
                                .append("<ROADIMPEDIMENTS>").append(unit.getUnitImpedimentRoad()).append("</ROADIMPEDIMENTS>")
                                .append("<GOODTHRU>").append(goodThru).append("</GOODTHRU>")
                                .append("<PORT_PTD>").append(ufv.getUfvPaidThruDay()).append("</PORT_PTD>")
                                .append("<PORT_LFD>").append(formatLfd).append("</PORT_LFD>")
                                .append("<DEMURRAGE>").append("</DEMURRAGE>")
                                .append("<LOCATIONDETAILS>").append(ufv.getUfvLastKnownPosition().toString()).append("</LOCATIONDETAILS>")
                                .append("<LOCATION>").append(ufv.getUfvLastKnownPosition().getPosLocType().getKey()).append("</LOCATION>")
                                .append("<SHIPPINGLINE>").append(unit.getUnitLineOperator().getBzuId()).append("</SHIPPINGLINE>")
                                .append("<SHIPPINGLINEDETAILS>").append(unit.getUnitLineOperator().getBzuName()).append("</SHIPPINGLINEDETAILS>")
                                .append("<DISCHARGED>").append(ufv.getUfvTimeIn()).append("</DISCHARGED>")
                                .append("<EQSZ>").append(equipment.getEqEquipType().getEqtypNominalLength().getKey()).append("</EQSZ>")
                                .append("<EQTP>").append(equipment.getEqEquipType().getEqtypIsoGroup().getKey()).append("</EQTP>")
                                .append("<EQHT>").append(equipment.getEqEquipType().getEqtypNominalHeight().getKey()).append("</EQHT>")
                                .append("<GROSSWEIGHT>").append(unit.getUnitGoodsAndCtrWtKg()).append("</GROSSWEIGHT>")
                                .append("<CATEGORY>").append(unit.getUnitCategory().getKey()).append("</CATEGORY>")
                                .append("<HAZ_CLASS>").append(unit.getUnitWorstHazardClass()).append("</HAZ_CLASS>")
                                .append("<CATEGORYDETAILS>").append(unit.getUnitCategory().getKey()).append("</CATEGORYDETAILS>")
                                .append("<STATUS>").append(unit.getUnitFreightKind().getKey()).append("</STATUS>")
                                .append("<STATUSDETAILS>").append(unit.getUnitFreightKind().getKey()).append("</STATUSDETAILS>")
                                .append("<OOGFL>").append(unit.getUnitIsOog()).append("</OOGFL>")
                                .append("<OUT_TIME>").append(ufv.getUfvTimeOut()).append("</OUT_TIME>")
                                .append("<VESSELVOYAGE>").append(ufv.getInboundCarrierVisit().getCvId()).append("</VESSELVOYAGE>")
                                .append("<VESSEL>").append(ufv.getInboundCarrierVisit().getCvCvd().getCarrierVehicleId()).append("</VESSEL>")
                                .append("<VOYAGE>").append(ufv.getInboundCarrierInVoyNbrTrainId()).append("</VOYAGE>")
                                .append("<TRUCKER>").append(trucker).append("</TRUCKER>")
                                .append("<TOTALCHARGES>").append(totalCharges).append("</TOTALCHARGES>")
                                .append("<GTD>").append(ufv.getUfvGuaranteeThruDay()).append("</GTD>")
                                .append("</TABLE>")
                    }
                }
            }
            return results.toString().replace("null", "").replace("NOM", "")
        } else if (gvyClassParam.toUpperCase().equals("BILLOFLADINGHEADERBYGKEY")) {
            StringBuilder results = new StringBuilder()

            BillOfLading billOfLading = BillOfLading.hydrate(gKeyParam)
            List<String> billHolds = new ArrayList<String>()
            ServicesManager serviceManager = (ServicesManager) Roastery.getBean(ServicesManager.BEAN_ID);
            if (serviceManager.getImpedimentsForEntity(billOfLading) != null) {
                Set<ServiceImpediment> impedSet = serviceManager.getImpedimentsForEntity(billOfLading)
                for (ServiceImpediment servImped : impedSet) {
                    if (servImped.getStatus().equals(FlagStatusEnum.ACTIVE)) {
                        billHolds.add(servImped.toString())
                    }
                }
            }
            String freight
            String customs
            String holds = billHolds.toString().replace("[", "").replace("]", "")
            if (billHolds == null) {
                freight = "RELEASED"
                customs = "RELEASED"
            } else {
                if (holds.toUpperCase().contains("FREIGHT")) {
                    freight = "HOLD"
                } else {
                    freight = "RELEASED"
                }
                if (holds.toUpperCase().contains("CUSTOMS")) {
                    customs = "HOLD"
                } else {
                    customs = "RELEASED"
                }
            }
            results.append("<TABLE>")
                    .append("<BILLOFLADING>").append(billOfLading.getBlNbr()).append("</BILLOFLADING>")
                    .append("<GKEY>").append(billOfLading.getBlGkey()).append("</GKEY>")
                    .append("<SHIPPINGLINE>").append(billOfLading.getBlLineOperator().getBzuId()).append("</SHIPPINGLINE>")
                    .append("<SHIPPINGLINEDETAILS>").append(billOfLading.getBlLineOperator().getBzuName()).append("</SHIPPINGLINEDETAILS>")
                    .append("<CUSTOMS>").append(customs).append("</CUSTOMS>")
                    .append("<FREIGHT>").append(freight).append("</FREIGHT>")
                    .append("<HOLDS>").append(holds).append("</HOLDS>")
                    .append("<VESSEL>").append(billOfLading.getBlCarrierVisit().getCarrierVehicleId()).append("</VESSEL>")
                    .append("<VESSELNAME>").append(billOfLading.getBlCarrierVisit().getCarrierVehicleName()).append("</VESSELNAME>")
                    .append("<VOYAGE>").append(billOfLading.getBlCarrierVisit().getCarrierIbVoyNbrOrTrainId()).append("</VOYAGE>")
                    .append("<NOTES>").append(billOfLading.getBlNotes()).append("</NOTES>")
                    .append("<PIECECOUNT>").append(billOfLading.getBlManifestedQty()).append("</PIECECOUNT>")
                    .append("<SHIPPER>").append(billOfLading.getShipperName()).append("</SHIPPER>")
                    .append("<DESTINATION>").append(billOfLading.getBlDestination()).append("</DESTINATION>")
                    .append("<ORIGIN>").append(billOfLading.getBlOrigin()).append("</ORIGIN>")
                    .append("<CREATED>").append(billOfLading.getBlCreated()).append("</CREATED>")
                    .append("<CREATOR>").append(billOfLading.getBlCreator()).append("</CREATOR>")
                    .append("</TABLE>")
            return results.toString().replace("null", "").replace("NOM", "")
        } else if (gvyClassParam.toUpperCase().equals("IMPORTINQUIRYBYBLGKEY")) {
            StringBuilder results = new StringBuilder()

            BillOfLading billOfLading = BillOfLading.hydrate(gKeyParam)
            List<String> billHolds = new ArrayList<String>()
            ServicesManager serviceManager = (ServicesManager) Roastery.getBean(ServicesManager.BEAN_ID);
            if (serviceManager.getImpedimentsForEntity(billOfLading) != null) {
                Set<ServiceImpediment> impedSet = serviceManager.getImpedimentsForEntity(billOfLading)
                for (ServiceImpediment servImped : impedSet) {
                    if (servImped.getStatus().equals(FlagStatusEnum.ACTIVE)) {
                        billHolds.add(servImped.toString())
                    }
                }
            }
            Set<BlGoodsBl> blGoodsBlSet = billOfLading.getBlBlGoodsBls()
            for (BlGoodsBl blGoodsBl : blGoodsBlSet) {
                GoodsBl goodsBl = blGoodsBl.getBlgdsblGoodsBl()
                Unit unit = goodsBl.getGdsUnit()
                Equipment equipment = unit.getPrimaryEq()
                UnitFacilityVisit ufv = unit.getUfvForEventTime(blGoodsBl.getBlgdsblCreated())// .getUnitActiveUfvNowActive()
                GoodsBase goodsBase = unit.getUnitGoods()
                GoodsBl goodsBls = GoodsBl.resolveGoodsBlFromGoodsBase(goodsBase)
                Set<BillOfLading> billOfLadingSet = goodsBls.getGdsblBillsOfLading()
                String bls = ""
                for (BillOfLading blNbr : billOfLadingSet) {
                    bls = bls + blNbr.getBlNbr() + ","
                }
                String readyFl
                if (unit.getUnitStoppedRoad()) {
                    readyFl = "NO"
                } else {
                    if (unit.getLocType().equals(LocTypeEnum.YARD)) {
                        readyFl = "YES"
                    } else {
                        readyFl = "NO"
                    }
                }
                String freight
                String customs
                String holds = billHolds.toString().replace("[", "").replace("]", "")
                if (billHolds == null) {
                    freight = "RELEASED"
                    customs = "RELEASED"
                } else {
                    if (holds.toUpperCase().contains("FREIGHT")) {
                        freight = "HOLD"
                    } else {
                        freight = "RELEASED"
                    }
                    if (holds.toUpperCase().contains("CUSTOMS")) {
                        customs = "HOLD"
                    } else {
                        customs = "RELEASED"
                    }
                }
                Date lfd = ufv.getUfvCalculatedLastFreeDayDate()
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                String formatLfd
                if (lfd == null) {
                    formatLfd = ""
                } else {
                    formatLfd = sdf.format(lfd)
                }
                String goodThru
                if (ufv.getUfvCalculatedLastFreeDayDate() > ufv.getUfvPaidThruDay()) {
                    goodThru = formatLfd
                } else {
                    goodThru = ufv.getUfvPaidThruDay().toString()
                }
                String trucker = (unit.getUnitRtgTruckingCompany() == null) ? "" : unit.getUnitRtgTruckingCompany().getBzuId() + " - " + unit.getUnitRtgTruckingCompany().getBzuName()
                MetafieldId aqi = MetafieldIdFactory.valueOf("customFlexFields.ufvCustomDFFAQIAmt")
                MetafieldId dem = MetafieldIdFactory.valueOf("customFlexFields.ufvCustomDFFDemurrageAmt")
                MetafieldId vacis = MetafieldIdFactory.valueOf("customFlexFields.ufvCustomDFFVacisAmt")
                MetafieldId other = MetafieldIdFactory.valueOf("customFlexFields.ufvCustomDFFOtherAmt")
                Double aqiAmt = (ufv.getField(aqi) == null) ? 0 : ufv.getField(aqi)
                Double demAmt = (ufv.getField(dem) == null) ? 0 : ufv.getField(dem)
                Double vacisAmt = (ufv.getField(vacis) == null) ? 0 : ufv.getField(vacis)
                Double otherAmt = (ufv.getField(other) == null) ? 0 : ufv.getField(other)
                Double totalCharges = aqiAmt + demAmt + vacisAmt + otherAmt


                results.append("<TABLE>")
                        .append("<GKEY>").append(unit.getUnitGkey()).append("</GKEY>")
                        .append("<EQUIPMENTNBR>").append(unit.getUnitId()).append("</EQUIPMENTNBR>")
                        .append("<BILLOFLADING>").append(bls.substring(0, bls.length() - 1)).append("</BILLOFLADING>")
                        .append("<READYFORDELIVERY>").append(readyFl).append("</READYFORDELIVERY>")
                        .append("<FREIGHT>").append(freight).append("</FREIGHT>")
                        .append("<CUSTOMS>").append(customs).append("</CUSTOMS>")
                        .append("<HOLDS>").append(unit.getUnitImpediments()).append("</HOLDS>")
                        .append("<GOODTHRU>").append(goodThru).append("</GOODTHRU>")
                        .append("<PORT_PTD>").append(ufv.getUfvPaidThruDay()).append("</PORT_PTD>")
                        .append("<PORT_LFD>").append(formatLfd).append("</PORT_LFD>")
                        .append("<DEMURRAGE>").append("</DEMURRAGE>")
                        .append("<LOCATIONDETAILS>").append(ufv.getUfvLastKnownPosition().toString()).append("</LOCATIONDETAILS>")
                        .append("<LOCATION>").append(ufv.getUfvLastKnownPosition().getPosLocType().getKey()).append("</LOCATION>")
                        .append("<SHIPPINGLINE>").append(unit.getUnitLineOperator().getBzuId()).append("</SHIPPINGLINE>")
                        .append("<SHIPPINGLINEDETAILS>").append(unit.getUnitLineOperator().getBzuName()).append("</SHIPPINGLINEDETAILS>")
                        .append("<DISCHARGED>").append(ufv.getUfvTimeIn()).append("</DISCHARGED>")
                        .append("<EQSZ>").append(equipment.getEqEquipType().getEqtypNominalLength().getKey()).append("</EQSZ>")
                        .append("<EQTP>").append(equipment.getEqEquipType().getEqtypIsoGroup().getKey()).append("</EQTP>")
                        .append("<EQHT>").append(equipment.getEqEquipType().getEqtypNominalHeight().getKey()).append("</EQHT>")
                        .append("<GROSSWEIGHT>").append(unit.getUnitGoodsAndCtrWtKg()).append("</GROSSWEIGHT>")
                        .append("<CATEGORY>").append(unit.getUnitCategory().getKey()).append("</CATEGORY>")
                        .append("<HAZ_CLASS>").append(unit.getUnitWorstHazardClass()).append("</HAZ_CLASS>")
                        .append("<CATEGORYDETAILS>").append(unit.getUnitCategory().getKey()).append("</CATEGORYDETAILS>")
                        .append("<STATUS>").append(unit.getUnitFreightKind().getKey()).append("</STATUS>")
                        .append("<STATUSDETAILS>").append(unit.getUnitFreightKind().getKey()).append("</STATUSDETAILS>")
                        .append("<OOGFL>").append(unit.getUnitIsOog()).append("</OOGFL>")
                        .append("<OUT_TIME>").append(ufv.getUfvTimeOut()).append("</OUT_TIME>")
                        .append("<VESSELVOYAGE>").append(ufv.getInboundCarrierVisit().getCvId()).append("</VESSELVOYAGE>")
                        .append("<VESSEL>").append(ufv.getInboundCarrierVisit().getCvCvd().getCarrierVehicleId()).append("</VESSEL>")
                        .append("<VOYAGE>").append(ufv.getInboundCarrierInVoyNbrTrainId()).append("</VOYAGE>")
                        .append("<TRUCKER>").append(trucker).append("</TRUCKER>")
                        .append("<TOTALCHARGES>").append(totalCharges).append("</TOTALCHARGES>")
                        .append("<GTD>").append(ufv.getUfvGuaranteeThruDay()).append("</GTD>")
                        .append("</TABLE>")
            }
            return results.toString().replace("null", "").replace("NOM", "")
        } else if (gvyClassParam.toUpperCase().equals("BILLOFLADINGHISTORYBYGKEY")) {
            StringBuilder results = new StringBuilder()

            BillOfLading billOfLading = BillOfLading.hydrate(gKeyParam)
            ServicesManager serviceManager = (ServicesManager) Roastery.getBean(ServicesManager.BEAN_ID);
            List<Event> events = serviceManager.getEventHistory(billOfLading)
            TreeSet<Event> treeSet = new TreeSet<Event>(new PerformedComparator())
            treeSet.addAll(events)
            for (Event event : treeSet) {

                results.append("<TABLE>")
                        .append("<SHIPPINGLINE>").append(billOfLading.getBlLineOperator().getBzuId()).append("</SHIPPINGLINE>")
                        .append("<PERFORMED>").append(event.getEventTime()).append("</PERFORMED>")
                        .append("<PERFORMER>").append(event.getEvntCreator()).append("</PERFORMER>")
                        .append("<EVENT>").append(event.getEventTypeId()).append("</EVENT>")
                        .append("<QTY>").append(event.getEvntQuantity()).append("</QTY>")
                        .append("<NOTES>").append(event.getEvntNote()).append("</NOTES>")
                        .append("<CHANGES>").append(event.getEvntFieldChangesString()).append("</CHANGES>")
                        .append("</TABLE>")
            }
            return results
        } else if (gvyClassParam.toUpperCase().equals("UPDATEBILLOFLADING")) {

            BillOfLading billOfLading = BillOfLading.hydrate(gKeyParam)
            ServicesManager serviceManager = (ServicesManager) Roastery.getBean(ServicesManager.BEAN_ID);
            if (serviceManager.getImpedimentsForEntity(billOfLading) != null) {
                Set<ServiceImpediment> impedSet = serviceManager.getImpedimentsForEntity(billOfLading)
                for (ServiceImpediment servImped : impedSet) {
                    if (servImped.getStatus().equals(FlagStatusEnum.REQUIRED) && servImped.getFlagType().getId().equals("FREIGHTBL")) {
                        serviceManager.applyPermission("FREIGHTBL", billOfLading, "", "", true)
                    }
                }
            }
            return "Bill of Lading Updated"
        } else if (gvyClassParam.toUpperCase().equals("DRAYCONTAINERS")) {
            StringBuilder results = new StringBuilder()
            if (eqNbrParam != null && eqNbrParam != "") {
                String[] equipArr = eqNbrParam.toUpperCase().split(",")
                for (String equip : equipArr) {
                    String[] lineArr = lineParam.toUpperCase().split(",")
                    for (String line : lineArr) {
                        Equipment equipment = Equipment.findEquipment(equip)
                        UnitFinder unitFinder = (UnitFinder) Roastery.getBean("unitFinder")
                        Unit unit = unitFinder.findActiveUnit(complex, equipment)
                        UnitFacilityVisit ufv = unit.getUfvForEventTime(unit.getUnitChanged())// .getUnitActiveUfvNowActive()
                        if (!unit.getUnitLineOperator().getBzuId().equalsIgnoreCase(line)) {
                            continue
                        }
                        String trucker = (unit.getUnitRtgTruckingCompany() == null) ? "" : unit.getUnitRtgTruckingCompany().getBzuId() + " | " + unit.getUnitRtgTruckingCompany().getBzuName()
                        String drayStatus = (unit.getUnitDrayStatus() == null) ? "" : unit.getUnitDrayStatus().getKey()
                        results.append("<TABLE>")
                                .append("<EQUIPMENTNBR>").append(unit.getUnitId()).append("</EQUIPMENTNBR>")
                                .append("<GKEY>").append(unit.getUnitGkey()).append("</GKEY>")
                                .append("<LOCATION>").append(ufv.getUfvLastKnownPosition().getPosLocType().getKey()).append("</LOCATION>")
                                .append("<POS_ID>").append(ufv.getUfvLastKnownPosition().toString()).append("</POS_ID>")
                                .append("<SHIPPINGLINE>").append(unit.getUnitLineOperator().getBzuId()).append("</SHIPPINGLINE>")
                                .append("<SHIPPINGLINEDETAILS>").append(unit.getUnitLineOperator().getBzuName()).append("</SHIPPINGLINEDETAILS>")
                                .append("<EQSZ>").append(equipment.getEqEquipType().getEqtypNominalLength().getKey()).append("</EQSZ>")
                                .append("<EQTP>").append(equipment.getEqEquipType().getEqtypIsoGroup().getKey()).append("</EQTP>")
                                .append("<EQHT>").append(equipment.getEqEquipType().getEqtypNominalHeight().getKey()).append("</EQHT>")
                                .append("<CATEGORY>").append(unit.getUnitCategory().getKey()).append("</CATEGORY>")
                                .append("<CATEGORYDETAILS>").append(unit.getUnitCategory().getKey()).append("</CATEGORYDETAILS>")
                                .append("<STATUS>").append(unit.getUnitFreightKind().getKey()).append("</STATUS>")
                                .append("<STATUSDETAILS>").append(unit.getUnitFreightKind().getKey()).append("</STATUSDETAILS>")
                                .append("<DRAYSTATUS>").append(drayStatus).append("</DRAYSTATUS>")
                                .append("<DRAYTRKCID>").append(trucker).append("</DRAYTRKCID>")
                                .append("</TABLE>")
                    }
                }
            } else {
                DomainQuery dq = QueryFactory.createDomainQuery(VesselEntity.VESSEL_VISIT_DETAILS)
                        .addDqPredicate(PredicateFactory.eq(VesselVisitField.VVD_VESSEL_ID, vesselParam))
                        .addDqPredicate(PredicateFactory.eq(VesselVisitField.VVD_IB_VYG_NBR, voyageParam))

                Set<VesselVisitDetails> vesselVisitDetailsSet = HibernateApi.getInstance().findEntitiesByDomainQuery(dq)
                for (VesselVisitDetails vvd in vesselVisitDetailsSet) {
                    CarrierVisit cv = vvd.getInboundCv()
                    DomainQuery dq2 = QueryFactory.createDomainQuery(InventoryEntity.UNIT)
                            .addDqPredicate(PredicateFactory.eq(InventoryField.UNIT_DECLARED_IB_CV, cv.getCvGkey()))

                    Set<Unit> unitSet = HibernateApi.getInstance().findEntitiesByDomainQuery(dq2)
                    for (Unit unit in unitSet) {
                        if (unit.getLocType().equals(LocTypeEnum.YARD) || unit.getLocType().equals(LocTypeEnum.VESSEL)) {
                            String[] lineArr = lineParam.toUpperCase().split(",")
                            for (String line : lineArr) {
                                UnitFacilityVisit ufv = unit.getUfvForEventTime(unit.getUnitChanged()) //.getUnitActiveUfvNowActive()
                                Equipment equipment = unit.getPrimaryEq()
                                if (!unit.getUnitLineOperator().getBzuId().equalsIgnoreCase(line)) {
                                    continue
                                }
                                String trucker = (unit.getUnitRtgTruckingCompany() == null) ? "" : unit.getUnitRtgTruckingCompany().getBzuId() + " - " + unit.getUnitRtgTruckingCompany().getBzuName()
                                String drayStatus = (unit.getUnitDrayStatus() == null) ? "" : unit.getUnitDrayStatus().getKey()
                                results.append("<TABLE>")
                                        .append("<EQUIPMENTNBR>").append(unit.getUnitId()).append("</EQUIPMENTNBR>")
                                        .append("<GKEY>").append(unit.getUnitGkey()).append("</GKEY>")
                                        .append("<LOCATION>").append(ufv.getUfvLastKnownPosition().getPosLocType().getKey()).append("</LOCATION>")
                                        .append("<POS_ID>").append(ufv.getUfvLastKnownPosition().toString()).append("</POS_ID>")
                                        .append("<SHIPPINGLINE>").append(unit.getUnitLineOperator().getBzuId()).append("</SHIPPINGLINE>")
                                        .append("<SHIPPINGLINEDETAILS>").append(unit.getUnitLineOperator().getBzuName()).append("</SHIPPINGLINEDETAILS>")
                                        .append("<EQSZ>").append(equipment.getEqEquipType().getEqtypNominalLength().getKey()).append("</EQSZ>")
                                        .append("<EQTP>").append(equipment.getEqEquipType().getEqtypIsoGroup().getKey()).append("</EQTP>")
                                        .append("<EQHT>").append(equipment.getEqEquipType().getEqtypNominalHeight().getKey()).append("</EQHT>")
                                        .append("<CATEGORY>").append(unit.getUnitCategory().getKey()).append("</CATEGORY>")
                                        .append("<CATEGORYDETAILS>").append(unit.getUnitCategory().getKey()).append("</CATEGORYDETAILS>")
                                        .append("<STATUS>").append(unit.getUnitFreightKind().getKey()).append("</STATUS>")
                                        .append("<STATUSDETAILS>").append(unit.getUnitFreightKind().getKey()).append("</STATUSDETAILS>")
                                        .append("<DRAYSTATUS>").append(drayStatus).append("</DRAYSTATUS>")
                                        .append("<DRAYTRKCID>").append(trucker).append("</DRAYTRKCID>")
                                        .append("</TABLE>")
                            }
                        }
                    }
                }
            }
            return results.toString().replace("null", "").replace("NOM", "")
        } else if (gvyClassParam.toUpperCase().equals("UPDATEDRAYCODE")) {
            Unit unit = Unit.hydrate(gKeyParam)
            FieldChanges fieldChanges = new FieldChanges()
            FieldChanges fieldChanges1 = new FieldChanges()
            if (drayStatusParam != "") {
                if (!unit.getUnitDrayStatus().equals(DrayStatusEnum.getEnum(drayStatusParam))) {
                    fieldChanges1.setFieldChange(UnitField.UNIT_DRAY_STATUS, DrayStatusEnum.getEnum(drayStatusParam), unit.getUnitDrayStatus())
                    unit.setSelfAndFieldChange(UnitField.UNIT_DRAY_STATUS, DrayStatusEnum.getEnum(drayStatusParam), fieldChanges1)
                    unit.recordUnitEvent(EventEnum.UNIT_REROUTE, fieldChanges1, null)
                }
            } else if (unit.getUnitDrayStatus() != null) {
                fieldChanges1.setFieldChange(UnitField.UNIT_DRAY_STATUS, unit.getUnitDrayStatus(), DrayStatusEnum.getEnum(drayStatusParam))
                unit.updateDrayStatus(null)
                unit.recordUnitEvent(EventEnum.UNIT_REROUTE, fieldChanges1, null)
            }
            if (drayTruckParam != "") {
                if (!unit.getUnitRtgTruckingCompany().equals(ScopedBizUnit.findScopedBizUnit(drayTruckParam, BizRoleEnum.HAULIER))) {
                    fieldChanges.setFieldChange(UnitField.UNIT_RTG_TRUCKING_COMPANY, ScopedBizUnit.findScopedBizUnit(drayTruckParam, BizRoleEnum.HAULIER), unit.getUnitRtgTruckingCompany())
                    unit.setSelfAndFieldChange(UnitField.UNIT_RTG_TRUCKING_COMPANY, ScopedBizUnit.findScopedBizUnit(drayTruckParam, BizRoleEnum.HAULIER), fieldChanges)
                    unit.recordUnitEvent(EventEnum.UNIT_TRUCKER_ASSIGNED, fieldChanges, null)
                    unit.recordUnitEvent(EventEnum.UPDATE_DELIVERY_RQMNTS, fieldChanges, "Delivery Requirements Updated")
                }
            } else if (unit.getUnitRtgTruckingCompany() != null) {
                fieldChanges.setFieldChange(UnitField.UNIT_RTG_TRUCKING_COMPANY, unit.getUnitRtgTruckingCompany(), ScopedBizUnit.findScopedBizUnit(drayTruckParam, BizRoleEnum.HAULIER))
                unit.setUnitRtgTruckingCompany(null)
                unit.recordUnitEvent(EventEnum.UNIT_TRUCKER_ASSIGNED, fieldChanges, null)
                unit.recordUnitEvent(EventEnum.UPDATE_DELIVERY_RQMNTS, fieldChanges, "Delivery Requirements Updated")
            }
            return "Dray Status and Trucker Updated"
        } else {
            return "Error:  Class Parameter not recognized"
        }
    }
}

private class PerformedComparator implements Comparator<Event> {
    public int compare(Event e1, Event e2) {
        return e1.getEventTime().compareTo(e2.getEventTime())
    }
}
