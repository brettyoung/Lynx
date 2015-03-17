import com.navis.apex.business.model.GroovyInjectionBase
import com.navis.argo.ContextHelper
import com.navis.argo.business.api.ArgoUtils
import com.navis.argo.business.atoms.*
import com.navis.argo.business.model.*
import com.navis.argo.business.reference.*
import com.navis.framework.business.Roastery
import com.navis.framework.persistence.HibernateApi
import com.navis.framework.portal.FieldChanges
import com.navis.framework.portal.UserContext
import com.navis.framework.portal.query.DomainQuery
import com.navis.framework.portal.query.PredicateFactory
import com.navis.framework.portal.query.QueryFactory
import com.navis.framework.util.BizViolation
import com.navis.inventory.InventoryEntity
import com.navis.inventory.InventoryField
import com.navis.inventory.business.api.UnitField
import com.navis.inventory.business.api.UnitFinder
import com.navis.inventory.business.atoms.UfvTransitStateEnum
import com.navis.inventory.business.atoms.UnitVisitStateEnum
import com.navis.inventory.business.imdg.HazardItem
import com.navis.inventory.business.imdg.Hazards
import com.navis.inventory.business.imdg.ImdgClass
import com.navis.inventory.business.units.*
import com.navis.orders.BookingField
import com.navis.orders.business.eqorders.Booking
import com.navis.orders.business.eqorders.EquipmentOrderItem
import com.navis.vessel.VesselEntity
import com.navis.vessel.api.VesselVisitField
import com.navis.vessel.business.schedule.VesselVisitDetails
import groovy.sql.Sql

import java.text.SimpleDateFormat

class BookingMaintenance extends GroovyInjectionBase {

    String url = 'jdbc:oracle:thin:@10.128.2.78:1521:INVENTORY'
    String user = 'gltinv'
    String password = 'sparcsn4'
    String driverClassName = 'oracle.jdbc.OracleDriver'

    public String execute(Map inParameters) throws BizViolation {

        Facility facility = ContextHelper.getThreadFacility()
        Complex complex = ContextHelper.getThreadComplex()
        String gKeyParam = inParameters.get("Gkey")
        String eqNbrParam = inParameters.get("Container")
        String typeParam = inParameters.get("Type")
        String hazIdParam = inParameters.get("Hazard Class")
        String filenameParam = inParameters.get("Filename")
        String userParam = inParameters.get("User")
        String unNbrParam = inParameters.get("UNDG")
        String limitedQtyParam = inParameters.get("Limited Qty")
        String seqParam = inParameters.get("Sequence")
        String bookingParam = inParameters.get("Booking")
        String subTypeParam = inParameters.get("Sub Type")
        String statusParam = inParameters.get("Status")
        String lineParam = inParameters.get("Line")
        String vesselParam = inParameters.get("Vessel")
        String voyageParam = inParameters.get("Voyage")
        String callNbrParam = inParameters.get("Call Nbr")
        String polParam = inParameters.get("POL")
        String podParam = inParameters.get("POD")
        String pod2Param = inParameters.get("POD2")
        String shipperParam = inParameters.get("Shipper")
        String consigneeParam = inParameters.get("Consignee")
        String originParam = inParameters.get("Origin")
        String destinationParam = inParameters.get("Destination")
        String truckParam = inParameters.get("Truck Id")
        String custPriorParam = inParameters.get("Customer Priority")
        String agentParam = inParameters.get("Agent")
        String sendRefNbr = inParameters.get("Sender Ref Nbr")
        String specialStowParam = inParameters.get("Special Stow")
        String msgFuncParam = inParameters.get("Msg function")
        String msgIdParam = inParameters.get("Msg Id")
        String optionalPointParam = inParameters.get("Optional Point Id")
        String refNbrParam = inParameters.get("Reference Nbr")
        String notesParam = inParameters.get("Notes")
        String poolMemberParam = inParameters.get("Pool Member")
        String custRefParam = inParameters.get("Customer Reference")
        String militaryBookParam = inParameters.get("Military Book")
        String militaryTCNParam = inParameters.get("Military TCN")
        String holdPartParam = inParameters.get("Hold Partial")
        String oogParam = inParameters.get("OOG")
        String reservedParam = inParameters.get("Reserved")
        String noFullInParam = inParameters.get("No Full In")
        String noEmptyOutParam = inParameters.get("No Empty Out")
        String eqClassParam = inParameters.get("Status")
        String eqSzParam = "NOM" + inParameters.get("Size")
        String eqIsoGroupParam = inParameters.get("ISO Group")
        String eqHtParam = "NOM" + inParameters.get("Height")
        String eqSzOrig = "NOM" + inParameters.get("Size Original")
        String eqIsoGroupOrig = inParameters.get("ISO Group Original")
        String eqHtOrig = "NOM" + inParameters.get("Height Original")
        String gradeIdParam = inParameters.get("Grade ID");
        String humidityParam = inParameters.get("Humidity");
        String qtyParam = inParameters.get("Qty")
        String tallyParam = inParameters.get("Tally")
        String tallyLimitParam = inParameters.get("Tally Limit")
        String tempParam = inParameters.get("Temperature")
        String tempUnitParam = inParameters.get("Temp Units")
        String ventParam = inParameters.get("Vent")
        String ventUnitParam = inParameters.get("Vent Units")
        String commodityParam = inParameters.get("Commodity")
        String hazGkeyParam = inParameters.get("Hazard ID")
        String hazDescParam = inParameters.get("Hazard Description")
        String packingGroupParam = inParameters.get("Packing Group")
        String oldEqoGkeyParam = inParameters.get("Old Gkey")
        String newEqoGkeyParam = inParameters.get("New Gkey")
        String imdgIdParam = inParameters.get("IMDG")
        String gvyClassParam = inParameters.get("Groovy Class")
        String actionParam = inParameters.get("Action")

        UserContext context = ContextHelper.getThreadUserContext()
        Date timeNow = ArgoUtils.convertDateToLocalDateTime(ArgoUtils.timeNow(), context.getTimeZone())
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
        String created = sdf.format(timeNow)

        Number errorCount = 0
        Number validCount = 0
        String results = "Web Service Successful"

        if (gvyClassParam.toUpperCase().equals("BOOKINGHEADERMAINTENANCE")) {
            SpecialStow specialStow = SpecialStow.findSpecialStow(specialStowParam)
            RoutingPoint optionalPoint = RoutingPoint.findRoutingPoint(optionalPointParam)
            ScopedBizUnit truckCo = ScopedBizUnit.findScopedBizUnit(truckParam, BizRoleEnum.HAULIER)

            ScopedBizUnit lineOperator = ScopedBizUnit.findEquipmentOperator(lineParam)
            RoutingPoint portOfDischarge = RoutingPoint.findRoutingPoint(podParam)
            RoutingPoint portOfDischarge2 = RoutingPoint.findRoutingPoint(pod2Param)
            RoutingPoint portOfLoad = RoutingPoint.findRoutingPoint(polParam)

            if (actionParam == "INSERT") {
                DomainQuery dq = QueryFactory.createDomainQuery(VesselEntity.VESSEL_VISIT_DETAILS)
                        .addDqPredicate(PredicateFactory.eq(VesselVisitField.VVD_OB_VYG_NBR, voyageParam))
                        .addDqPredicate(PredicateFactory.eq(VesselVisitField.VVD_VESSEL_ID, vesselParam))

                List<VesselVisitDetails> vesselVisitDetailsSet = HibernateApi.getInstance().findEntitiesByDomainQuery(dq)

                VesselVisitDetails visitDetails = vesselVisitDetailsSet.get(0)

                CarrierVisit obCarrierVisit = visitDetails.getCvdCv()
                Booking.create(bookingParam, lineOperator, obCarrierVisit, FreightKindEnum.FCL, portOfLoad, portOfDischarge, null)
                Booking booking = Booking.findBookingByUniquenessCriteria(bookingParam, lineOperator, obCarrierVisit)
                booking.setFieldValue(BookingField.EQO_POD2, portOfDischarge2)
                if (shipperParam != null) {
                    Shipper shipper = Shipper.findOrCreateShipperByIdOrName(shipperParam)
                    booking.setFieldValue(BookingField.EQO_SHIPPER, shipper)
                };
                if (consigneeParam != null) {
                    Shipper consignee = Shipper.findOrCreateShipperByIdOrName(consigneeParam)
                    booking.setFieldValue(BookingField.EQO_CONSIGNEE, consignee)
                };
                booking.updateOriginAndDestination(originParam, destinationParam)
                booking.setFieldValue(BookingField.EQO_HAZARDS, hazIdParam)
                booking.setFieldValue(BookingField.EQO_TRUCK_CO, truckCo)
                if (agentParam != null) {
                    ScopedBizUnit agent = ScopedBizUnit.findScopedBizUnit(agentParam, BizRoleEnum.AGENT)
                    booking.setFieldValue(BookingField.EQO_AGENT, agent)
                }
                booking.setSpecialStow(specialStow)
                booking.setFieldValue(BookingField.EQO_POD_OPTIONAL, optionalPoint)
                booking.setFieldValue(BookingField.EQO_CLIENT_REF_NO, refNbrParam)
                booking.setFieldValue(BookingField.EQO_NOTES, notesParam)
            } else if (actionParam == "UPDATE") {
                Booking booking = Booking.hydrate(gKeyParam)
                booking.setFieldValue(BookingField.EQO_LINE_ID, lineParam)

                DomainQuery dq = QueryFactory.createDomainQuery(VesselEntity.VESSEL_VISIT_DETAILS)
                        .addDqPredicate(PredicateFactory.eq(VesselVisitField.VVD_OB_VYG_NBR, voyageParam))
                        .addDqPredicate(PredicateFactory.eq(VesselVisitField.VVD_VESSEL_ID, vesselParam))

                List<VesselVisitDetails> vesselVisitDetailsSet = HibernateApi.getInstance().findEntitiesByDomainQuery(dq)

                VesselVisitDetails visitDetails = vesselVisitDetailsSet.get(0)

                FieldChanges fieldChanges = new FieldChanges()
                FieldChanges fieldChanges1 = new FieldChanges()
                fieldChanges1.setFieldChange(BookingField.EQO_VESSEL_VISIT, visitDetails, booking.getEqoVesselVisit())
                fieldChanges.setFieldChange(BookingField.EQO_POD1, portOfDischarge, booking.getEqoPod1())
                booking.setSelfAndFieldChange(BookingField.EQO_VESSEL_VISIT, visitDetails.getCvdCv().getCvGkey(), fieldChanges1)
                booking.setSelfAndFieldChange(BookingField.EQO_POD1, portOfDischarge.getPointGkey(), fieldChanges)
                booking.setFieldValue(BookingField.EQO_LINE_ID, lineParam)
                booking.setFieldValue(BookingField.EQO_POL, portOfLoad)
                booking.setFieldValue(BookingField.EQO_POD2, portOfDischarge2)
                booking.setFieldValue(BookingField.EQO_POD1, portOfDischarge)
                if (shipperParam != "") {
                    Shipper shipper = Shipper.findOrCreateShipperByIdOrName(shipperParam)
                    booking.setFieldValue(BookingField.EQO_SHIPPER, shipper)
                } else {
                    booking.setFieldValue(BookingField.EQO_SHIPPER, null)
                }
                if (consigneeParam != "" && consigneeParam != booking.getConsigneeAsString()) {
                    Shipper consignee = Shipper.findOrCreateShipperByIdOrName(consigneeParam)
                    booking.setFieldValue(BookingField.EQO_CONSIGNEE, consignee)
                } else if (consigneeParam == "") {
                    booking.setFieldValue(BookingField.EQO_CONSIGNEE, null)
                }
                if (destinationParam != "" && destinationParam != booking.getEqoDestination()) {
                    booking.setFieldValue(BookingField.EQO_DESTINATION, destinationParam)
                } else if (destinationParam == "") {
                    booking.setFieldValue(BookingField.EQO_DESTINATION, null)
                }
                if (originParam != "" && originParam != booking.getEqoOrigin()) {
                    booking.setFieldValue(BookingField.EQO_ORIGIN, originParam)
                } else if (originParam == "") {
                    booking.setFieldValue(BookingField.EQO_ORIGIN, null)
                }
                booking.setFieldValue(BookingField.EQO_TRUCK_CO, truckCo)
                if (agentParam != "") {
                    ScopedBizUnit agent = ScopedBizUnit.findScopedBizUnit(agentParam, BizRoleEnum.AGENT)
                    booking.setFieldValue(BookingField.EQO_AGENT, agent)
                } else {
                    booking.setFieldValue(BookingField.EQO_AGENT, null)
                }
                booking.setSpecialStow(specialStow)
                booking.setFieldValue(BookingField.EQO_POD_OPTIONAL, optionalPoint)
                if (notesParam != "" && notesParam != booking.getEqoNotes()) {
                    booking.setFieldValue(BookingField.EQO_NOTES, notesParam)
                }
                if (custRefParam != "" && custRefParam != booking.getEqoClientRefNo()) {
                    booking.setFieldValue(BookingField.EQO_CLIENT_REF_NO, custRefParam)
                }
                booking.setFieldValue(BookingField.EQO_HOLD_PARTIALS, holdPartParam)
                booking.setFieldValue(BookingField.EQO_DISPATCH_RESERVED, reservedParam)
                booking.setFieldValue(BookingField.EQO_MILITARY_TCN, militaryTCNParam)

            } else if (actionParam == "DELETE") {
                Booking booking = Booking.hydrate(gKeyParam)
                try {
                    booking.setFieldValue(BookingField.EQO_HAZARDS, null)
                    HibernateApi.getInstance().delete(booking)
                    HibernateApi.getInstance().flush()
                } catch (Exception e) {
                    return "Error:  Cannot delete a booking with associated equipment"
                }
            }

        } else if (gvyClassParam.toUpperCase().equals("BOOKINGEQUIPMENTMAINTENANCE")) {
            Booking booking = Booking.hydrate(gKeyParam)
            if (actionParam == "DELETE") {

                Set<EquipmentOrderItem> orderItems = booking.getEqboOrderItems()
                if (orderItems.isEmpty()) {
                    return "Error:  No Order Item found"
                }
                Set<EquipmentOrderItem> orderItemToDelete = new HashSet<EquipmentOrderItem>()
                UnitFinder unitFinder = (UnitFinder) Roastery.getBean("unitFinder")
                for (EquipmentOrderItem item in orderItems) {
                    if (item.getEqoiEqSize().equals(EquipNominalLengthEnum.getEnum(eqSzParam))
                            && item.getEqoiEqIsoGroup().equals(EquipIsoGroupEnum.getEnum(eqIsoGroupParam))
                            && item.getEqoiEqHeight().equals(EquipNominalHeightEnum.getEnum(eqHtParam))) {
                        if (!item.getOrderItemReservedUnits().isEmpty() || !item.getOrderItemDeliveredUnits().isEmpty() ||
                                !item.getOrderItemTbdUnits().isEmpty() || unitFinder.findUnitsAdvisedOrReceivedForOrderItem(item).size() > 0) {
                            return "Error:  Equipment is Associated with Booking Item"
                        }
                        orderItemToDelete.add(item)
                    }
                }
                booking.removeBookingItems(orderItemToDelete)
            } else if (actionParam == "INSERT") {
                EquipmentOrderItem orderItem = EquipmentOrderItem.create(booking, qtyParam.toLong(), EquipNominalLengthEnum.getEnum(eqSzParam), EquipIsoGroupEnum.getEnum(eqIsoGroupParam), EquipNominalHeightEnum.getEnum(eqHtParam))
                orderItem.setFieldValue(BookingField.EQOI_EQ_GRADE, gradeIdParam)
                if (humidityParam != "") {
                    orderItem.setFieldValue(BookingField.EQOI_HUMIDITY_REQUIRED, humidityParam)
                } else orderItem.setFieldValue(BookingField.EQOI_HUMIDITY_REQUIRED, null)
                orderItem.setFieldValue(BookingField.EQOI_TALLY, tallyParam)
                if (tempParam != "") {
                    orderItem.setFieldValue(BookingField.EQOI_TEMP_REQUIRED, tempParam)
                } else {
                    orderItem.setFieldValue(BookingField.EQOI_TEMP_REQUIRED, null)
                }
                if (ventParam != "") {
                    orderItem.setFieldValue(BookingField.EQOI_VENT_REQUIRED, ventParam)
                } else {
                    orderItem.setFieldValue(BookingField.EQOI_VENT_REQUIRED, null)
                }
                if (ventUnitParam != "") {
                    orderItem.setFieldValue(BookingField.EQOI_VENT_UNIT, VentUnitEnum.getEnum(ventUnitParam))
                } else (orderItem.setFieldValue(BookingField.EQOI_VENT_UNIT, null))
                if (commodityParam != "") {
                    // orderItem.setFieldValue(BookingField.EQOI_COMMODITY, Commodity.findCommodity(commodityParam))
                    orderItem.setFieldValue(BookingField.EQOI_COMMODITY_DESC, commodityParam)
                } else {
                    //     orderItem.setFieldValue(BookingField.EQOI_COMMODITY, null)
                    orderItem.setFieldValue(BookingField.EQOI_COMMODITY_DESC, null)
                }
            } else if (actionParam == "UPDATE") {
                EquipmentOrderItem orderItem = EquipmentOrderItem.findOrCreateOrderItem(booking, qtyParam.toLong(), EquipNominalLengthEnum.getEnum(eqSzOrig), EquipIsoGroupEnum.getEnum(eqIsoGroupOrig), EquipNominalHeightEnum.getEnum(eqHtOrig))
                orderItem.setFieldValue(BookingField.EQOI_EQ_SIZE, EquipNominalLengthEnum.getEnum(eqSzParam))
                orderItem.setFieldValue(BookingField.EQOI_EQ_ISO_GROUP, EquipIsoGroupEnum.getEnum(eqIsoGroupParam))
                orderItem.setFieldValue(BookingField.EQOI_EQ_HEIGHT, EquipNominalHeightEnum.getEnum(eqHtParam))
                orderItem.setFieldValue(BookingField.EQOI_EQ_GRADE, EquipGrade.findEquipGrade(gradeIdParam))
                if (humidityParam != "") {
                    orderItem.setFieldValue(BookingField.EQOI_HUMIDITY_REQUIRED, humidityParam)
                } else orderItem.setFieldValue(BookingField.EQOI_HUMIDITY_REQUIRED, null)
                orderItem.setFieldValue(BookingField.EQOI_QTY, qtyParam)
                orderItem.setFieldValue(BookingField.EQOI_TALLY, tallyParam)
                orderItem.setFieldValue(BookingField.EQOI_TALLY_LIMIT, tallyLimitParam)
                if (tempParam != "") {
                    orderItem.setFieldValue(BookingField.EQOI_TEMP_REQUIRED, tempParam)
                } else {
                    orderItem.setFieldValue(BookingField.EQOI_TEMP_REQUIRED, null)
                }
                if (ventParam != "") {
                    orderItem.setFieldValue(BookingField.EQOI_VENT_REQUIRED, ventParam)
                } else {
                    orderItem.setFieldValue(BookingField.EQOI_VENT_REQUIRED, null)
                }
                if (ventUnitParam != "") {
                    orderItem.setFieldValue(BookingField.EQOI_VENT_UNIT, VentUnitEnum.getEnum(ventUnitParam))
                } else (orderItem.setFieldValue(BookingField.EQOI_VENT_UNIT, null))
                if (commodityParam != "") {
                    //  orderItem.setFieldValue(BookingField.EQOI_COMMODITY, Commodity.findCommodity(commodityParam))
                    orderItem.setFieldValue(BookingField.EQOI_COMMODITY_DESC, commodityParam)
                } else {
                    //  orderItem.setFieldValue(BookingField.EQOI_COMMODITY, null)
                    orderItem.setFieldValue(BookingField.EQOI_COMMODITY_DESC, null)
                }
                if (oogParam != "") {
                    orderItem.setFieldValue(BookingField.EQOI_IS_OOG, oogParam)
                } else {
                    orderItem.setFieldValue(BookingField.EQOI_IS_OOG, null)
                }
            }
        } else if (gvyClassParam.toUpperCase().equals("BOOKINGRESERVATIONS")) {

            Equipment equipment = Equipment.findEquipment(eqNbrParam)

            Booking booking = Booking.hydrate(gKeyParam)

            if (actionParam.equals("DELETE")) {

                UnitFinder unitFinder = (UnitFinder) Roastery.getBean("unitFinder")
                Unit unit
                unit = unitFinder.findActiveUnit(complex, equipment)
                if (unit == null) {
                    unit = unitFinder.findDepartedUnit(complex, equipment)
                }
                UnitFacilityVisit ufv = unit.getUfvForFacilityNewest(facility)
                String itemType
                if (unit.getUnitFreightKind() == FreightKindEnum.MTY
                        && ufv.getUfvTransitState() == UfvTransitStateEnum.S70_DEPARTED) {
                    itemType = "Empty Out"
                } else if (unit.isReservedForBooking()) {
                    itemType = "Reserved"
                } else if (unit.getUnitFreightKind() == FreightKindEnum.FCL
                        && ufv.getUfvTransitState() != UfvTransitStateEnum.S10_ADVISED
                        && ufv.getUfvTransitState() != UfvTransitStateEnum.S20_INBOUND) {
                    itemType = "Full In"
                } else if (unit.isUnitPreadvised()) {
                    itemType = "Preadvised"
                } else {
                    return "Error:  Item type undefined"
                }

                if (itemType.equals("Reserved")) {
                    unit.cancelReservation()
                    return "Reservation Cancelled"
                }
                if (itemType.equals("Preadvised")) {
                    if (unit.unitIsHazard) {
                        return "Error:  Not allowed to delete a preadvised hazardous container from this screen"
                    }
                    unit.cancelPreadvise()
                    unit.recordUnitEvent(EventEnum.UNIT_CANCEL_PREADVISE, null, null)
                    return "Preadvise Cancelled"
                }
                if (itemType.equals("Full In")) {
                    return "Error:  Not allowed to delete a full in container"
                }
                if (itemType.equals("Empty Out")) {
                    return "Error:  Not allowed to delete an empty out container"
                }

            } else if (actionParam.equals("INSERT")) {

                if (equipment == null && typeParam != "PREADVISE") {
                    return "Error:  Container does not exist"
                }
                Set<EquipmentOrderItem> orderItems = booking.getEqboOrderItems()
                if (orderItems.isEmpty()) {
                    return "Error:  No Order Item found"
                }
                for (EquipmentOrderItem item in orderItems) {
                    if (typeParam.equals("RESERVED")) {
                        UnitFacilityVisit ufv = findActiveUfv(eqNbrParam)
                        Unit unit = ufv.getUfvUnit()
                        UnitEquipment unitEquipment = unit.getUnitPrimaryUe()
                        EquipType eqType = equipment.getEqEquipType()
                        if (item.getEqoiEqSize().equals(eqType.getEqtypNominalLength())
                                && item.getEqoiEqIsoGroup().equals(eqType.getEqtypIsoGroup())
                                && item.getEqoiEqHeight().equals(eqType.getEqtypNominalHeight())
                                && unit.isUnitInYard()) {
                            validCount = 1
                            unit.reserveForOrder(item, unitEquipment)
                            return "Container Reserved Successfully"
                        }


                    } else if (typeParam.equals("PREADVISE")) {

                        if (equipment == null) {
                            Number itemTypes = 0
                            Set<EquipmentOrderItem> orderItemCheck = booking.getEqboOrderItems()
                                for (EquipmentOrderItem itemCheck in orderItemCheck) {
                                    EquipType equipCheck = EquipType.findEquipType(itemCheck.getEqoiEqHeight(), itemCheck.getEqoiEqIsoGroup(), itemCheck.getEqoiEqSize(), EquipClassEnum.CONTAINER)
                                    if (equipCheck != null) {
                                        List<GeneralReference> genRefs = GeneralReference.findAllEntriesById("LYNX HAZISO", equipCheck.getEqtypArchetype().getEqtypId())
                                        if (genRefs.size() > 0) {
                                            itemTypes = itemTypes + 1
                                        }
                                    }
                                }

                            if (itemTypes > 1) {
                                return "Error:  Container not on file.  Unable to determine SZ/TP/HT"
                            }
                            itemTypes = 0

                            EquipType equipType = EquipType.findEquipType(item.getEqoiEqHeight(), item.getEqoiEqIsoGroup(), item.getEqoiEqSize(), EquipClassEnum.CONTAINER)
                            if (equipType == null) {
                                return "Error:  Equipment Type " + item.getEqoiEqHeight().toString() + "/" + item.getEqoiEqIsoGroup().toString() + "/" + item.getEqoiEqSize().toString() + " not on file"
                            }
                            List<GeneralReference> genRefs = GeneralReference.findAllEntriesById("LYNX HAZISO", equipType.getEqtypArchetype().getEqtypId())
                            if (genRefs.size().equals(0)) {
                                return "Error:  Container does not exist"
                            }
                            LineOperator unkLine = LineOperator.findLineOperatorById("UNK")
                            Container container = Container.findOrCreateContainer(eqNbrParam, DataSourceEnum.USER_WEB)
                            container.setEqEquipType(equipType)
                            container.setEqObsoleteOwner(MasterBizUnit.findBizUnit("UNK", BizRoleEnum.LINEOP))
                            HibernateApi.getInstance().saveOrUpdate(container)
                            EquipmentState eqState = EquipmentState.findOrCreateEquipmentState(Equipment.findEquipment(container.getEqIdFull()), Operator.findOperator("GCT"))
                            eqState.setEqsEqOwner(unkLine)
                            eqState.setEqsEqOperator(booking.getEqoLine())
                            HibernateApi.getInstance().saveOrUpdate(eqState)
                            equipment = Equipment.findEquipment(container.getEqIdFull())
                        }
                        CarrierVisit cv = CarrierVisit.findOrCreateGenericCv(complex, LocTypeEnum.TRUCK)
                        UnitFinder unitFinder = (UnitFinder) Roastery.getBean("unitFinder")

                        EquipType eqType = equipment.getEqEquipType()
                        Unit unit1 = unitFinder.findActiveUnit(complex, equipment)
                        if (unit1 != null) {
                            if (unit1.getUnitVisitState().equals(UnitVisitStateEnum.ACTIVE)) {
// update hazard information if container already preadvised
                                if (booking.getEqboNbr() != unit1.getDepartureOrder().getEqboNbr()) {
                                    return "Error:  Container " + eqNbrParam + " is currently peadvised against Booking " + unit1.getDepartureOrder().getEqboNbr()
                                }
                                if (!booking.isHazardous()) {
                                    return "Error:  Cannot preadvise unit " + eqNbrParam + " because this unit is already active"
                                }
                                if (unit1.getUnitIsHazard()) {
                                    deleteHazards(unit1)
                                }
                                insertHazards(unit1, booking, hazIdParam, imdgIdParam, unNbrParam, filenameParam, userParam)
                                unit1.recordUnitEvent(EventEnum.UNIT_HAZARDS_INSERT, null, "Hazard Docs Uploaded")
                                return "Container Preadvised Successfully"
                            }
                        }
                        String hazGkey
                        if (hazIdParam == "" && imdgIdParam == "" && unNbrParam == "" && booking.isHazardous()) {
                            errorCount = errorCount + 1
                            return "Error:  Haz Class Must Be Selected"
                        }
                        if (equipment.getEquipmentOwnerId() != booking.getEqoLine().getBzuId() && equipment.getEquipmentOwnerId() != "UNK") {
                            errorCount = errorCount + 1
                            return "Error:  Container Owner Does Not Match Booking Owner"
                        }
                        CarrierVisit cvBooking = booking.getEqoVesselVisit()
                        VisitDetails visitDetails = cvBooking.getCvCvd()
                        VesselVisitDetails vesselVisitDetails = VesselVisitDetails.loadByGkey(visitDetails.getPrimaryKey().toString().toLong())
                        if (vesselVisitDetails.getVvdTimeHazCutoff() != null && vesselVisitDetails.getVvdTimeHazCutoff().before(timeNow)) {
                            return "Error:  Vessel Visit is past hazardous cutoff"
                        }
                        if (item.getEqoiEqSize().equals(eqType.getEqtypNominalLength())
                                && item.getEqoiEqIsoGroup().equals(eqType.getEqtypIsoGroup())
                                && item.getEqoiEqHeight().equals(eqType.getEqtypNominalHeight())
                                && errorCount < 1) {
                            validCount = 1
                            Unit unit = Unit.createContainerizedUnit(eqNbrParam, complex, CarrierVisit.findOrCreateGenericCv(complex, LocTypeEnum.TRUCK), booking.getEqoVesselVisit(), equipment, equipment.getEquipmentOperator())
                            UnitEquipment unitEquipment = unit.getUnitPrimaryUe()
                            unitEquipment.setUeOrderItem(item)
                            LocPosition locPosition = LocPosition.createTruckPosition(cv, "", "")
                            UnitFacilityVisit newUfv = UnitFacilityVisit.createUnitFacilityVisit(unit, facility, locPosition, cv)
                            unit.setUnitActiveUfv(newUfv)

                            if (booking.isHazardous()) {
                                insertHazards(unit, booking, hazIdParam, imdgIdParam, unNbrParam, filenameParam, userParam)
                                unit.recordUnitEvent(EventEnum.UNIT_HAZARDS_INSERT, null, "Hazard Docs Uploaded")
                                item.preadviseUnit(unit)
                                unit.setFieldValue(InventoryField.UNIT_CATEGORY, UnitCategoryEnum.EXPORT)
                                unit.makeActive()
                                newUfv.makeActive()
                                newUfv.setFieldValue(InventoryField.UFV_TRANSIT_STATE, UfvTransitStateEnum.S20_INBOUND)
                                unit.recordUnitEvent(EventEnum.UNIT_HAZARDS_UPDATE, null, "Hazards Updated")
                                unit.recordUnitEvent(EventEnum.UNIT_PREADVISE, null, null)
                                unit.setUnitRouting(booking.getRoutingInfo())
                                return "Container and Hazard Preadvised Successfully"
                            } else {
                                item.preadviseUnit(unit)
                                unit.setFieldValue(InventoryField.UNIT_CATEGORY, UnitCategoryEnum.EXPORT)
                                unit.makeActive()
                                newUfv.makeActive()
                                newUfv.setFieldValue(InventoryField.UFV_TRANSIT_STATE, UfvTransitStateEnum.S20_INBOUND)
                                if (hazIdParam != "") {
                                    unit.recordUnitEvent(EventEnum.UNIT_HAZARDS_UPDATE, null, "Hazards Updated")
                                }
                                unit.recordUnitEvent(EventEnum.UNIT_PREADVISE, null, null)
                                unit.setUnitRouting(booking.getRoutingInfo())
                                return "Container Preadvised Successfully"
                            }
                        }
                    }
                }
                if (validCount == 0) {
                    return "Error:  Container does not match booking details"
                }

            }
        } else if (gvyClassParam.toUpperCase().equals("HAZPREADVISEDELETE")) {
            UnitFacilityVisit ufv = findActiveUfv(eqNbrParam)
            Unit unit = ufv.getUfvUnit()

            unit.cancelPreadvise()
            unit.recordUnitEvent(EventEnum.UNIT_CANCEL_PREADVISE, null, null)
            HibernateApi.getInstance().flush()
            deleteHazards(unit)

        } else if (gvyClassParam.toUpperCase().equals("MAINTAINHAZDESC")) {
            Sql sqlMaintainHazDesc = Sql.newInstance(url, user, password, driverClassName)

            if (actionParam.equals("DELETE")) {
                String deleteHazDesc = """
                        delete from versiant_csp_hazdescriptions
                              where id = ${hazGkeyParam}
                        """
                sqlMaintainHazDesc.execute(deleteHazDesc)
            } else if (actionParam.equals("UPDATE")) {
                String updateHazDesc = """
                         update versiant_csp_hazdescriptions
                            set haz_class     = '${hazIdParam}',
                                description   = '${hazDescParam}',
                                unnumber      = '${unNbrParam}',
                                packinggroup  = '${packingGroupParam}',
                                changed       = sysdate,
                                changer       = '${userParam}'
                          where id = ${hazGkeyParam}
                         """
                sqlMaintainHazDesc.executeUpdate(updateHazDesc)
            } else if (actionParam.equals("INSERT")) {
                String insertHazDesc = """
                        insert into versiant_csp_hazdescriptions(ID,HAZ_CLASS,DESCRIPTION,UNNUMBER,PACKINGGROUP,CREATED,CREATOR)
                        values ((select max(id) + 1 from versiant_csp_hazdescriptions),
                                '${hazIdParam}','${hazDescParam}','${unNbrParam}','${packingGroupParam}',sysdate,'${
                    userParam
                }')
                        """
                sqlMaintainHazDesc.executeInsert(insertHazDesc)
            }

        } else if (gvyClassParam.toUpperCase().equals("BOOKINGHAZARDS")) {
            Booking booking = Booking.hydrate(gKeyParam)

            if (actionParam.equals("INSERT")) {
                if (unNbrParam.length() > 4) {
                    return "Error:  UN Number cannot be more than 4 characters"
                }
                if (booking.isHazardous()) {
                    Hazards hazards = booking.getEqoHazards()
                    List<HazardItem> checkHazList = hazards.getHzrdItems()
                    for (HazardItem checkHaz in checkHazList) {
                        if (checkHaz.getHzrdiImdgClass().getKey() == hazIdParam &&
                                checkHaz.getHzrdiUNnum() == unNbrParam) {
                            errorCount = errorCount + 1
                        }
                    }
                    if (errorCount > 0) {
                        results = "Error:  Hazard Item already exists"
                        return results
                    }
                    hazards.addHazardItem(ImdgClass.getEnum(hazIdParam), unNbrParam)
                    List<HazardItem> hazardItems = hazards.getHzrdItems()
                    for (HazardItem hazardItem in hazardItems) {
                        if (hazardItem.getHzrdiImdgClass() == ImdgClass.getEnum(hazIdParam) &&
                                hazardItem.getHzrdiUNnum() == unNbrParam) {
                            hazardItem.setFieldValue(InventoryField.HZRDI_LTD_QTY, limitedQtyParam.toBoolean())
                        }
                    }
                } else {
                    Hazards hazards = Hazards.createHazardsEntity()
                    booking.setFieldValue(BookingField.EQO_HAZARDS, hazards)
                    hazards.attachHazardToEntity("Booking", booking.getEqboGkey())
                    hazards.addHazardItem(ImdgClass.getEnum(hazIdParam), unNbrParam)
                    List<HazardItem> hazardItems = hazards.getHzrdItems()
                    for (HazardItem hazardItem in hazardItems) {
                        if (hazardItem.getHzrdiImdgClass() == ImdgClass.getEnum(hazIdParam) &&
                                hazardItem.getHzrdiUNnum() == unNbrParam) {
                            hazardItem.setFieldValue(InventoryField.HZRDI_LTD_QTY, limitedQtyParam.toBoolean())
                        }
                    }
                }
            } else if (actionParam.equals("UPDATE")) {
                Hazards hazards = booking.getEqoHazards()
                List<HazardItem> hazardItems = hazards.getHzrdItems()
                for (HazardItem hazardItem in hazardItems) {
                    if (hazardItem.getHzrdiSeq() == seqParam.toBigInteger()) {
                        hazardItem.setFieldValue(InventoryField.HZRDI_LTD_QTY, limitedQtyParam.toBoolean())
                    }
                }
            } else if (actionParam.equals("DELETE")) {
                Hazards hazards = booking.getEqoHazards()

                DomainQuery dq = QueryFactory.createDomainQuery(InventoryEntity.HAZARD_ITEM)
                        .addDqPredicate(PredicateFactory.eq(InventoryField.HZRDI_HAZARDS, hazards.getHzrdGkey()))
                        .addDqPredicate(PredicateFactory.eq(InventoryField.HZRDI_SEQ, seqParam.toBigInteger()))

                List<HazardItem> hazItemToDelete = HibernateApi.getInstance().findEntitiesByDomainQuery(dq)
                HazardItem hazItemDel = hazItemToDelete.get(0)
                hazards.deleteHazardItem(hazItemDel)

                List remainingHazItems = hazards.getHzrdItems()

                if (remainingHazItems.size() == 0) {
                    booking.setFieldValue(BookingField.EQO_HAZARDS, null)
                    HibernateApi.getInstance().delete(hazards)
                    HibernateApi.getInstance().flush()
                }
            }
        } else if (gvyClassParam.toUpperCase().equals("SPLITBOOKING")) {
            Booking oldBooking = Booking.hydrate(oldEqoGkeyParam)
            Booking newBooking = Booking.hydrate(newEqoGkeyParam)
            String[] eqNbrArr = eqNbrParam.split(",")
            if (oldBooking.isHazardous() || newBooking.isHazardous()) {
                return "Error:  Cannot Roll To or From Hazardous Bookings"
            }
            Number qtyCheck = 0
            Number eqTypeCheck = 0
            Set<EquipmentOrderItem> orderItemQty = newBooking.getEqboOrderItems()
            if (orderItemQty.isEmpty()) {
                return "Error:  No Order Item Found"
            }
            for (EquipmentOrderItem itemQty in orderItemQty) {
                for (String eqNbr : eqNbrArr) {
                    Equipment eq = Equipment.findEquipment(eqNbr)
                    if (itemQty.getEqoiEqSize().equals(eq.getEqEquipType().getEqtypNominalLength())
                            && itemQty.getEqoiEqIsoGroup().equals(eq.getEqIsoGroup())
                            && itemQty.getEqoiEqHeight().equals(eq.getEqEquipType().getEqtypNominalHeight())) {
                        qtyCheck = qtyCheck + 1
                    }
                    if (qtyCheck + itemQty.getEqoiTallyReceive() > itemQty.getEqoiQty()) {
                        return "Error: Quantity Will Be Exceeded For Item " + itemQty.getEqoiEqSize().getKey().replace("NOM", "") + "/" + itemQty.getEqoiEqIsoGroup().getKey() + "/" + itemQty.getEqoiEqHeight().getKey().replace("NOM", "")
                    }
                }
                qtyCheck = 0
            }
            for (String eqNbr : eqNbrArr) {
                Equipment equipment = Equipment.findEquipment(eqNbr)
                UnitFinder unitFinder = (UnitFinder) Roastery.getBean("unitFinder")
                Unit unit = unitFinder.findActiveUnit(complex, equipment)
                UnitFacilityVisit ufv = unit.getUnitActiveUfvNowActive()
                Set<EquipmentOrderItem> orderItems = newBooking.getEqboOrderItems()
                for (EquipmentOrderItem item in orderItems) {
                    if (item.getEqoiEqSize().equals(equipment.getEqEquipType().getEqtypNominalLength())
                            && item.getEqoiEqIsoGroup().equals(equipment.getEqIsoGroup())
                            && item.getEqoiEqHeight().equals(equipment.getEqEquipType().getEqtypNominalHeight())) {
                        eqTypeCheck = 1
                        try {
                            unit.assignToOrder(item, equipment)
                            ufv.updateObCv(newBooking.getEqoVesselVisit())
                            FieldChanges fieldChanges = new FieldChanges()
                            fieldChanges.setFieldChange(UnitField.UNIT_DEPARTURE_ORDER, oldBooking.getEqboNbr(), newBooking.getEqboNbr())
                            FieldChanges fieldChanges1 = new FieldChanges()
                            fieldChanges1.setFieldChange(UnitField.UNIT_DECLARED_OB_CV, oldBooking.getEqoVesselVisit(), newBooking.getEqoVesselVisit())
                            fieldChanges.addFieldChanges(fieldChanges1)
                            unit.updateRouting(newBooking.getRoutingInfo())
                            unit.recordUnitEvent(EventEnum.UNIT_ROLL, fieldChanges, null)
                        } catch (Exception e) {
                            return "Error:  " + e.getMessage()
                        }
                    }
                }
                if (eqTypeCheck.equals(0)) {
                    return "Error:  Equipment Type for Container " + eqNbr + " Doesn't Exist in Booking " + newBooking.getEqboNbr()
                }
            }
            return "Containers Successfully Rolled to New Booking"
        }
        return results
    }

    private void deleteHazards(Unit inUnit) {

        Sql sqlDeleteHaz = Sql.newInstance(url, user, password, driverClassName)
        String deleteHazDoc = """
                        delete from versiant_csp_hazardousdocs
                              where equse_gkey = ${inUnit.getUnitGkey()}
                        """
        sqlDeleteHaz.execute(deleteHazDoc)

        String updateGoods = """
                         update inv_goods
                            set hazards_gkey=null
                          where gkey=${inUnit.getUnitGoods().getGdsGkey()}
                         """
        sqlDeleteHaz.executeUpdate(updateGoods)

        String deleteHazItems = """
                        delete from inv_hazard_items
                         where exists ( select 1
                                          from inv_hazards haz
                                         where haz.gkey=hzrd_gkey
                                           and haz.owner_entity='GoodsBase'
                                           and haz.owner_gkey=${inUnit.getUnitGoods().getGdsGkey()})
                        """
        sqlDeleteHaz.execute(deleteHazItems)

        String deleteHaz = """
                        delete from inv_hazards
                              where owner_entity='GoodsBase'
                                and owner_gkey=${inUnit.getUnitGoods().getGdsGkey()}
                        """
        sqlDeleteHaz.execute(deleteHaz)
    }

    private void insertHazards(Unit inUnit, Booking inBooking, String inHazId, String inImdg, String inUnNbr, String inFilename, String inUser) {
        Sql sqlInsertHazDocs = Sql.newInstance(url, user, password, driverClassName)
        String insertHazDocs = """
                           Insert into versiant_csp_hazardousDocs(Id,Equse_Gkey,Eq_Nbr,Haz_Class,Undg_Nbr,Filenames,Creator,Created)
                           values (versiant_csp_hazdocsseq.Nextval, '${inUnit.getUnitGkey()}', '${
            inUnit.getUnitId()
        }', '${
            inHazId
        }', '${
            inUnNbr
        }', '${inFilename}', '${inUser}', sysdate)
                        """
        sqlInsertHazDocs.executeInsert(insertHazDocs)
        if (inHazId != "") {
            String[] hazArr = inHazId.split(";")
            Hazards hazards = Hazards.createHazardsEntity()
            for (String hazId : hazArr) {
                hazards.attachHazardToEntity("GoodsBase", inUnit.getUnitGoods().getGdsGkey())

                Sql sqlInsertHaz = Sql.newInstance(url, user, password, driverClassName)
                sqlInsertHaz.eachRow("""
                            select (select MAX(h.gkey) from inv_hazards h where h.owner_entity='GoodsBase' and h.owner_gkey=${
                    inUnit.getUnitGoods().getGdsGkey()
                }) hzrd_gkey,
                                   d.haz_class,
                                   d.unnumber,
                                   hi1.ltd_qty,
                                   d.haz_class,
                                   d.packinggroup,
                                   0,
                                   substr(d.description,1,70),
                                   case when nvl((select max(nvl(hi2.hazard_item_seq,0))+1
                                                         from inv_hazard_items hi2
                                                         join inv_hazards h2 on h2.gkey=hi2.hzrd_gkey
                                                        where h2.owner_gkey=${inUnit.getUnitGoods().getGdsGkey()}
                                                          and h2.owner_entity='GoodsBase'),0) = 0
                                        then rownum
                                        else nvl((select max(nvl(hi2.hazard_item_seq,0))+1
                                                         from inv_hazard_items hi2
                                                         join inv_hazards h2 on h2.gkey=hi2.hzrd_gkey
                                                        where h2.owner_gkey=${inUnit.getUnitGoods().getGdsGkey()}
                                                          and h2.owner_entity='GoodsBase'),0) + 1 end
                              from versiant_csp_hazdescriptions d
                              join inv_hazards haz1 on haz1.owner_gkey=${inBooking.getEqboGkey()}
                                                        and haz1.owner_entity='Booking'
                              join inv_hazard_items hi1 on hi1.hzrd_gkey=haz1.gkey
                                                            and hi1.imdg_code=d.haz_class
                                                            and hi1.un_num=d.unnumber
                             where not exists ( select 1
                                                  from inv_hazards haz
                                                  join inv_hazard_items hi on hi.hzrd_gkey=haz.gkey
                                                 where haz.owner_gkey=${inUnit.getUnitGoods().getGdsGkey()}
                                                   and haz.owner_entity='GoodsBase'
                                                   and hi.imdg_code=d.haz_class
                                                   and hi.un_num=d.unnumber )
                               and d.id = '${hazId}'
                        """) {
                    hazards.addHazardItem(ImdgClass.getEnum(it.HAZ_CLASS), it.UNNUMBER)
                    GoodsBase goods = inUnit.getUnitGoods()
                    goods.setFieldValue(InventoryField.GDS_HAZARDS, hazards)
                    goods.setFieldValue(InventoryField.GDS_IS_HAZARDOUS, true)
                    goods.setFieldValue(InventoryField.GDS_IMDG_TYPES, it.HAZ_CLASS)
                }
            }
        } else {
            String[] imdgArr = inImdg.split(";")
            String[] unArr = inUnNbr.split(";")
            Hazards hazards = Hazards.createHazardsEntity()
            int i = 0;
            for (String imdgId : imdgArr) {
                String un = unArr[i]// .get(i)
                hazards.attachHazardToEntity("GoodsBase", inUnit.getUnitGoods().getGdsGkey())
                Sql sqlInsertHaz = Sql.newInstance(url, user, password, driverClassName)
                sqlInsertHaz.eachRow("""
                            select (select MAX(h.gkey) from inv_hazards h where h.owner_entity='GoodsBase' and h.owner_gkey=${
                    inUnit.getUnitGoods().getGdsGkey()
                }) hzrd_gkey,
                                   hi1.imdg_class haz_class,
                                   hi1.un_num unnumber,
                                   hi1.ltd_qty,
                                   hi1.packing_group packinggroup,
                                   0,
                                   case when nvl((select max(nvl(hi2.hazard_item_seq,0))+1
                                                         from inv_hazard_items hi2
                                                         join inv_hazards h2 on h2.gkey=hi2.hzrd_gkey
                                                        where h2.owner_gkey=${inUnit.getUnitGoods().getGdsGkey()}
                                                          and h2.owner_entity='GoodsBase'),0) = 0
                                        then rownum
                                        else nvl((select max(nvl(hi2.hazard_item_seq,0))+1
                                                         from inv_hazard_items hi2
                                                         join inv_hazards h2 on h2.gkey=hi2.hzrd_gkey
                                                        where h2.owner_gkey=${inUnit.getUnitGoods().getGdsGkey()}
                                                          and h2.owner_entity='GoodsBase'),0) + 1 end
                              from inv_hazards haz1
                              join inv_hazard_items hi1 on hi1.hzrd_gkey=haz1.gkey
                                                            and hi1.imdg_code='${imdgId}'
                                                            and hi1.un_num='${un}'
                             where not exists ( select 1
                                                  from inv_hazards haz
                                                  join inv_hazard_items hi on hi.hzrd_gkey=haz.gkey
                                                 where haz.owner_gkey=${inUnit.getUnitGoods().getGdsGkey()}
                                                   and haz.owner_entity='GoodsBase'
                                                   and hi.imdg_code='${imdgId}'
                                                   and hi.un_num='${un}' )
                               and haz1.owner_gkey=${inBooking.getEqboGkey()}
                               and haz1.owner_entity='Booking'
                        """) {
                    hazards.addHazardItem(ImdgClass.getEnum(it.HAZ_CLASS), it.UNNUMBER)
                    GoodsBase goods = inUnit.getUnitGoods()
                    goods.setFieldValue(InventoryField.GDS_HAZARDS, hazards)
                    goods.setFieldValue(InventoryField.GDS_IS_HAZARDOUS, true)
                    goods.setFieldValue(InventoryField.GDS_IMDG_TYPES, it.HAZ_CLASS)
                }
                i++;
            }
        }
    }
}
