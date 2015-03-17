import com.navis.argo.*
import com.navis.argo.business.api.ArgoUtils
import com.navis.argo.business.api.GroovyApi
import com.navis.argo.business.atoms.*
import com.navis.argo.business.extract.ChargeableUnitEvent
import com.navis.argo.business.extract.Guarantee
import com.navis.argo.business.extract.billing.ConfigurationProperties
import com.navis.argo.business.model.CarrierVisit
import com.navis.argo.business.model.Complex
import com.navis.argo.business.model.Facility
import com.navis.argo.business.model.VisitDetails
import com.navis.argo.business.reference.*
import com.navis.argo.portal.BillingWsApiConsts
import com.navis.argo.util.XmlUtil
import com.navis.argo.webservice.types.v1_0.*
import com.navis.framework.business.Roastery
import com.navis.framework.persistence.HibernateApi
import com.navis.framework.portal.FieldChanges
import com.navis.framework.portal.UserContext
import com.navis.framework.portal.query.DomainQuery
import com.navis.framework.portal.query.PredicateFactory
import com.navis.framework.portal.query.QueryFactory
import com.navis.framework.util.BizFailure
import com.navis.framework.util.BizViolation
import com.navis.inventory.InventoryPropertyKeys
import com.navis.inventory.business.api.UnitField
import com.navis.inventory.business.api.UnitFinder
import com.navis.inventory.business.atoms.EqDamageSeverityEnum
import com.navis.inventory.business.units.EquipmentState
import com.navis.inventory.business.units.Unit
import com.navis.inventory.business.units.UnitEquipment
import com.navis.inventory.business.units.UnitFacilityVisit
import com.navis.orders.OrdersField
import com.navis.orders.business.atoms.EquipmentOrderDispatchReservedEnum
import com.navis.orders.business.eqorders.EquipmentDeliveryOrder
import com.navis.orders.business.eqorders.EquipmentOrderItem
import com.navis.www.services.argoservice.ArgoServiceLocator
import com.navis.www.services.argoservice.ArgoServicePort
import groovy.sql.Sql
import org.jdom.Element
import org.jdom.Text
import javax.xml.rpc.ServiceException
import javax.xml.rpc.Stub
import java.text.DateFormat
import java.text.SimpleDateFormat

class EquipmentMaintenance extends GroovyApi {
    public String execute(Map inParameters) {

        String urlBilling = 'jdbc:oracle:thin:@10.128.2.78:1521:BILLING'
        String userBilling = 'gltbill'
        String passwordBilling = 'sparcsn4'
        String driverClassNameBilling = 'oracle.jdbc.OracleDriver'

        Facility facility = ContextHelper.getThreadFacility()
        Complex complex = ContextHelper.getThreadComplex()
        String gKeyParam = inParameters.get("Gkey")
        String eqNbrParam = inParameters.get("Container")
        String severityParam = inParameters.get("Severity")
        String gradeParam = inParameters.get("Grade")
        String lineParam = inParameters.get("Line")
        String edoParam = inParameters.get("EDO")
        String eventParam = inParameters.get("Event")
        String destinationParam = inParameters.get("Destination")
        String truckParam = inParameters.get("Truck Id")
        String shipperParam = inParameters.get("Shipper")
        String notesParam = inParameters.get("Notes")
        String reservedParam = inParameters.get("Reserved")
        String preventParam = inParameters.get("Prevent Eq Substitution")
        String estimatedDateParam = inParameters.get("Estimated Date")
        String startDateParam = inParameters.get("Start Date")
        String endDateParam = inParameters.get("End Date")
        String eqSzParam = "NOM" + inParameters.get("Size")
        String eqIsoGroupParam = inParameters.get("ISO Group")
        String eqHtParam = "NOM" + inParameters.get("Height")
        String qtyParam = inParameters.get("Qty")
        String humidityParam = inParameters.get("Humidity")
        String tallyParam = inParameters.get("Tally")
        String tempParam = inParameters.get("Temperature")
        String ventParam = inParameters.get("Vent")
        String ventUnitParam = inParameters.get("Vent Units")
        String ptdParam = inParameters.get("Paid Through Date")
        String purposeParam = inParameters.get("Purpose")
        String draftNbrParam = inParameters.get("Draft Number")
        String paymentTypeParam = inParameters.get("Payment Type")
        String bankNameParam = inParameters.get("Bank Name")
        String checkNbrParam = inParameters.get("Check Number")
        String checkRtgNbrParam = inParameters.get("Check Routing Number")
        String checkDateParam = inParameters.get("Check Date")
        String paymentAmtParam = inParameters.get("Amount")
        String eventTypeParam = inParameters.get("Event Type")
        String payeeParam = inParameters.get("Payee")
        String payeeRoleParam = inParameters.get("Payee Role")
        String invItemGkeysParam = inParameters.get("Invoice Item Gkeys")
        String gvyClassParam = inParameters.get("Groovy Class")
        String actionParam = inParameters.get("Action")

        UserContext context = ContextHelper.getThreadUserContext()
        Date timeNow = ArgoUtils.convertDateToLocalDateTime(ArgoUtils.timeNow(), context.getTimeZone())
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
        String changed = sdf.format(timeNow)
        DateFormat formatter = new SimpleDateFormat("dd-MMM-yy")

        Number errorCount = 0
        StringBuilder resultBuilder = new StringBuilder()
        String results = "Web Service Successful"

        if (gvyClassParam.toUpperCase().equals("EQUIPMENTDAMAGEUPDATE")) {
            Equipment equipment = Equipment.findEquipment(eqNbrParam)
            UnitFinder unitFinder = (UnitFinder) Roastery.getBean("unitFinder")
            Unit unit = unitFinder.findActiveUnit(complex, equipment)
            EquipDamageType equipDamageType = EquipDamageType.findEquipDamageType("TEST", equipment.getEqClass())
            EqComponent eqComponent = EqComponent.findEqComponent("TEST", equipment.getEqClass())
            EqDamageSeverityEnum severityEnum = EqDamageSeverityEnum.getEnum(severityParam.toUpperCase())
            unit.addDamageItem(equipment, equipDamageType, eqComponent, severityEnum, timeNow, null)
        } else if (gvyClassParam.toUpperCase().equals("PTDUPDATE")) {
            Equipment equipment = Equipment.findEquipment(eqNbrParam)
            UnitFinder unitFinder = (UnitFinder) Roastery.getBean("unitFinder")
            Unit unit = unitFinder.findActiveUnit(complex, equipment)
            UnitFacilityVisit ufv = unit.getUnitActiveUfvNowActive()
            Date ptdDate = (Date) formatter.parse(ptdParam)
            ufv.setFieldValue(UnitField.UFV_PAID_THRU_DAY, ptdDate)
        } else if (gvyClassParam.toUpperCase().equals("EQUIPMENTGRADEUPDATE")) {
            Equipment equipment = Equipment.findEquipment(eqNbrParam)
            UnitFinder unitFinder = (UnitFinder) Roastery.getBean("unitFinder")
            Unit unit = unitFinder.findActiveUnit(complex, equipment)
            UnitEquipment unitEquipment = unit.getUnitPrimaryUe()
            EquipmentState eqState = unitEquipment.getUeEquipmentState()
            EquipGrade grade
            FieldChanges fieldChanges = new FieldChanges()
            FieldChanges fieldChanges1 = new FieldChanges()
            if (gradeParam != "") {
                grade = EquipGrade.findEquipGrade(gradeParam)
                if (grade == null) {
                    return "Error:  Equipment grade does not exist"
                }
                if (!eqState.getEqsGradeID().equals(grade)) {
                    fieldChanges1.setFieldChange(UnitField.UE_EQ_GRADE, eqState.getEqsGradeID(), grade)
                    eqState.updateGradeID(grade)
                    unit.recordUnitEvent(EventEnum.UNIT_PROPERTY_UPDATE, fieldChanges1, null)
                }
            } else if (eqState.getEqsGradeID() != null) {
                fieldChanges1.setFieldChange(UnitField.UE_EQ_GRADE, eqState.getEqsGradeID(), null)
                eqState.updateGradeID(null)
                unit.recordUnitEvent(EventEnum.UNIT_PROPERTY_UPDATE, fieldChanges1, null)
            }
            if (notesParam != "") {
                if (!unit.getUnitRemark().equals(notesParam)) {
                    fieldChanges.setFieldChange(UnitField.UNIT_REMARK, unit.getUnitRemark(), notesParam)
                    unit.updateRemarks(notesParam)
                    unit.recordUnitEvent(EventEnum.UNIT_PROPERTY_UPDATE, fieldChanges, null)
                }
            } else if (unit.getUnitRemark() != null) {
                fieldChanges.setFieldChange(UnitField.UNIT_REMARK, unit.getUnitRemark(), null)
                unit.updateRemarks(null)
                unit.recordUnitEvent(EventEnum.UNIT_PROPERTY_UPDATE, fieldChanges, null)
            }
            return "Update Successful"
        } else if (gvyClassParam.toUpperCase().equals("EDOHEADERMAINTENANCE")) {
            ScopedBizUnit truckCo = ScopedBizUnit.findScopedBizUnit(truckParam, BizRoleEnum.HAULIER)
            ScopedBizUnit lineOperator = ScopedBizUnit.findEquipmentOperator(lineParam)

            if (actionParam == "INSERT") {
                EquipmentDeliveryOrder.create(edoParam.toUpperCase(), lineOperator, null, null)
            } else if (actionParam == "UPDATE") {
                EquipmentDeliveryOrder edo = EquipmentDeliveryOrder.hydrate(gKeyParam)
                if (edoParam != "" && edoParam.toUpperCase() != edo.getEqboNbr()) {
                    edo.updateEqboNbr(edoParam.toUpperCase())
                }
                if (eventParam != "" && EventEnum.getEnum(eventParam) != edo.getEqoServiceType()) {
                    EventEnum event = EventEnum.getEnum(eventParam)
                    edo.setFieldValue(OrdersField.EQO_SERVICE_TYPE, event)
                } else if (eventParam == "") {
                    edo.setFieldValue(OrdersField.EQO_SERVICE_TYPE, null)
                }
                if (purposeParam != "" && OrderPurpose.findOrderPurpose(purposeParam) != edo.getEqoOrderPurpose()) {
                    OrderPurpose purpose = OrderPurpose.findOrderPurpose(purposeParam)
                    edo.setFieldValue(OrdersField.EQO_ORDER_PURPOSE, purpose.getOrderpurposeGkey())
                } else if (purposeParam == "") {
                    edo.setFieldValue(OrdersField.EQO_ORDER_PURPOSE, null)
                }
                if (destinationParam != "" && destinationParam != edo.getEqoDestination()) {
                    edo.setFieldValue(OrdersField.EQO_DESTINATION, destinationParam)
                } else if (destinationParam == "") {
                    edo.setFieldValue(OrdersField.EQO_DESTINATION, null)
                }
                if (truckParam != "" && truckCo != edo.getEqoTruckCo()) {
                    edo.setFieldValue(OrdersField.EQO_TRUCK_CO, truckCo)
                } else if (truckParam == "") {
                    edo.setFieldValue(OrdersField.EQO_TRUCK_CO, null)
                }
                if (shipperParam != "" && shipperParam != edo.getShipperAsString()) {
                    //   Shipper shipper = Shipper.findShipper(shipperParam)
                    edo.updateEqoShipper(shipperParam)
                } else if (shipperParam == "") {
                    edo.setFieldValue(OrdersField.EQO_SHIPPER, null)
                }
                if (notesParam != "" && notesParam != edo.getEqoNotes()) {
                    edo.setFieldValue(OrdersField.EQO_NOTES, notesParam)
                } else if (notesParam == "") {
                    edo.setFieldValue(OrdersField.EQO_NOTES, null)
                }
                edo.setFieldValue(OrdersField.EQO_DISPATCH_RESERVED, EquipmentOrderDispatchReservedEnum.getEnum(reservedParam))
                edo.setFieldValue(OrdersField.EQO_PREVENT_TYPE_SUBST, preventParam)
                if (estimatedDateParam != "") {
                    Date estimatedDate = (Date) formatter.parse(estimatedDateParam)
                    if (estimatedDate != edo.getEqoEstimatedDate()) {
                        edo.setFieldValue(OrdersField.EQO_ESTIMATED_DATE, estimatedDate)
                    }
                } else if (estimatedDateParam == "") {
                    edo.setFieldValue(OrdersField.EQO_ESTIMATED_DATE, null)
                }
                if (startDateParam != "") {
                    Date startDate = (Date) formatter.parse(startDateParam)
                    if (startDate != edo.getEqoEarliestDate()) {
                        edo.setFieldValue(OrdersField.EQO_EARLIEST_DATE, startDate)
                    }
                } else if (startDateParam == "") {
                    edo.setFieldValue(OrdersField.EQO_EARLIEST_DATE, null)
                }
                if (endDateParam != "") {
                    Date endDate = (Date) formatter.parse(endDateParam)
                    if (endDate != edo.getEqoLatestDate()) {
                        edo.setFieldValue(OrdersField.EQO_LATEST_DATE, endDate)
                    }
                } else if (endDateParam == "") {
                    edo.setFieldValue(OrdersField.EQO_LATEST_DATE, null)
                }
            } else if (actionParam == "DELETE") {
                EquipmentDeliveryOrder edo = EquipmentDeliveryOrder.hydrate(gKeyParam)
                try {
                    HibernateApi.getInstance().delete(edo)
                    HibernateApi.getInstance().flush()
                } catch (Exception e) {
                    return "Error:  Cannot delete an EDO with associated equipment"
                }
            }
        } else if (gvyClassParam.toUpperCase().equals("EDOEQUIPMENTMAINTENANCE")) {
            EquipmentDeliveryOrder edo = EquipmentDeliveryOrder.hydrate(gKeyParam)
            if (actionParam == "DELETE") {

                Set<EquipmentOrderItem> orderItems = edo.getEqboOrderItems()
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
                edo.removeEdoItems(orderItemToDelete)
            } else if (actionParam == "INSERT") {
                EquipmentOrderItem orderItem = EquipmentOrderItem.create(edo, qtyParam.toLong(), EquipNominalLengthEnum.getEnum(eqSzParam), EquipIsoGroupEnum.getEnum(eqIsoGroupParam), EquipNominalHeightEnum.getEnum(eqHtParam))
                EquipGrade grade = EquipGrade.findEquipGrade(gradeParam)
                orderItem.setFieldValue(OrdersField.EQOI_EQ_GRADE, grade)
                if (humidityParam != "") {
                    orderItem.setFieldValue(OrdersField.EQOI_HUMIDITY_REQUIRED, humidityParam)
                } else orderItem.setFieldValue(OrdersField.EQOI_HUMIDITY_REQUIRED, null)
                orderItem.setFieldValue(OrdersField.EQOI_TALLY, tallyParam)
                if (tempParam != "") {
                    orderItem.setFieldValue(OrdersField.EQOI_TEMP_REQUIRED, tempParam)
                } else {
                    orderItem.setFieldValue(OrdersField.EQOI_TEMP_REQUIRED, null)
                }
                if (ventParam != "") {
                    orderItem.setFieldValue(OrdersField.EQOI_VENT_REQUIRED, ventParam)
                } else {
                    orderItem.setFieldValue(OrdersField.EQOI_VENT_REQUIRED, null)
                }
                if (ventUnitParam != "") {
                    orderItem.setFieldValue(OrdersField.EQOI_VENT_UNIT, VentUnitEnum.getEnum(ventUnitParam))
                } else (orderItem.setFieldValue(OrdersField.EQOI_VENT_UNIT, null))
            } else if (actionParam == "UPDATE") {
                EquipmentOrderItem orderItem = EquipmentOrderItem.findOrCreateOrderItem(edo, qtyParam.toLong(), EquipNominalLengthEnum.getEnum(eqSzParam), EquipIsoGroupEnum.getEnum(eqIsoGroupParam), EquipNominalHeightEnum.getEnum(eqHtParam))
                orderItem.setFieldValue(OrdersField.EQOI_EQ_GRADE, EquipGrade.findEquipGrade(gradeParam))
                if (humidityParam != "") {
                    orderItem.setFieldValue(OrdersField.EQOI_HUMIDITY_REQUIRED, humidityParam)
                } else orderItem.setFieldValue(OrdersField.EQOI_HUMIDITY_REQUIRED, null)
                orderItem.setFieldValue(OrdersField.EQOI_QTY, qtyParam)
                if (tempParam != "") {
                    orderItem.setFieldValue(OrdersField.EQOI_TEMP_REQUIRED, tempParam)
                } else {
                    orderItem.setFieldValue(OrdersField.EQOI_TEMP_REQUIRED, null)
                }
                if (ventParam != "") {
                    orderItem.setFieldValue(OrdersField.EQOI_VENT_REQUIRED, ventParam)
                } else {
                    orderItem.setFieldValue(OrdersField.EQOI_VENT_REQUIRED, null)
                }
                if (ventUnitParam != "") {
                    orderItem.setFieldValue(OrdersField.EQOI_VENT_UNIT, VentUnitEnum.getEnum(ventUnitParam))
                } else {
                    orderItem.setFieldValue(OrdersField.EQOI_VENT_UNIT, null)
                }
            }
        } else if (gvyClassParam.toUpperCase().equals("EDORESERVEEQ")) {
            EquipmentDeliveryOrder edo = EquipmentDeliveryOrder.hydrate(gKeyParam)
            Equipment equipment = Equipment.findEquipment(eqNbrParam)
            UnitFinder unitFinder = (UnitFinder) Roastery.getBean("unitFinder")
            Unit unit = unitFinder.findActiveUnit(complex, equipment)
            Set<EquipmentOrderItem> orderItems = edo.getEqboOrderItems()
            if (orderItems.isEmpty()) {
                return "Error:  No Order Item found"
            }
            if (actionParam == "DELETE") {
                for (EquipmentOrderItem item in orderItems) {
                    if (item.getEqoiEqSize().equals(equipment.getEqEquipType().getEqtypNominalLength())
                            && item.getEqoiEqIsoGroup().equals(equipment.getEqEquipType().getEqtypIsoGroup())
                            && item.getEqoiEqHeight().equals(equipment.getEqEquipType().getEqtypNominalHeight())) {
                        item.cancelReservation(unit)
                    }
                }
            } else {
                for (EquipmentOrderItem item in orderItems) {
                    if (item.getEqoiEqSize().equals(equipment.getEqEquipType().getEqtypNominalLength())
                            && item.getEqoiEqIsoGroup().equals(equipment.getEqEquipType().getEqtypIsoGroup())
                            && item.getEqoiEqHeight().equals(equipment.getEqEquipType().getEqtypNominalHeight())) {
                        item.reserveEq(equipment)
                    }
                }
            }
        } else if (gvyClassParam.toUpperCase().equals("CONTAINERCHARGES")) {
            Equipment equipment = Equipment.findEquipment(eqNbrParam)
            UnitFinder unitFinder = (UnitFinder) Roastery.getBean("unitFinder")
            Unit unit = unitFinder.findActiveUnit(complex, equipment)
            UnitFacilityVisit ufv = unit.getUnitActiveUfvNowActive()
            CarrierVisit cv = ufv.getInboundCarrierVisit()
            String inCallNbr = (cv == null) ? "" : cv.getCarrierIbVisitCallNbr()
            String vesselName = (cv == null) ? "" : cv.getCarrierVehicleName()
            Date faDate
            VisitDetails vd
            if (cv == null) {
                vd = cv.getCvCvd()
                faDate = vd.getCvdTimeFirstAvailability()
            }
            Date proposedPTD = (Date) formatter.parse(ptdParam)
            String[] lineArr = lineParam.toUpperCase().split(",")
            for (String line : lineArr) {
                if (!unit.getUnitLineOperator().getBzuId().equalsIgnoreCase(line)) {
                    continue
                }
                EdiInvoice inv = queryUnitChargesInv(ufv, proposedPTD, payeeParam)
                List<InvoiceCharge> invCharges = inv.getInvoiceChargeArray()
                if (invCharges.isEmpty()) {
                    deleteInvoice(inv.getDraftNumber())
                    return "Error:  No Charges Outstanding for this Container"
                }
                TreeSet<InvoiceCharge> treeSet = new TreeSet<InvoiceCharge>(new ChargesComparator())
                treeSet.addAll(invCharges)
                for (InvoiceCharge invCharge : treeSet) {
                    String toDate = invCharge.getChargeEventTypeId().equals("STORAGE") ? invCharge.getEventPerformedTo().getTime().toString() : "";
                    resultBuilder.append("<TABLE>")
                            .append("<GKEY>").append(unit.getUnitGkey()).append("</GKEY>")
                            .append("<EQUIPMENTNBR>").append(invCharge.getChargeEntityId()).append("</EQUIPMENTNBR>")
                            .append("<CHARGEEVENTTYPE>").append(invCharge.getChargeEventTypeId()).append("</CHARGEEVENTTYPE>")
                            .append("<FROMDATE>").append(invCharge.getEventPerformedFrom().getTime().toString()).append("</FROMDATE>")
                            .append("<TODATE>").append(toDate).append("</TODATE>")
                            .append("<DESCRIPTION>").append(invCharge.getDescription()).append("</DESCRIPTION>")
                            .append("<AMOUNT>").append(invCharge.getTotalCharged()).append("</AMOUNT>")
                            .append("<QUANTITY>").append(invCharge.getQuantity()).append("</QUANTITY>")
                            .append("<DRAFTNBR>").append(inv.getDraftNumber()).append("</DRAFTNBR>")
                            .append("<BILLOFLADING>").append(unit.getUnitGoods().getGdsBlNbr()).append("</BILLOFLADING>")
                            .append("<LOCATION>").append(unit.getUnitCalculatedCurrentPositionName()).append("</LOCATION>")
                            .append("<PTD>").append(ufv.getUfvPaidThruDay()).append("</PTD>")
                            .append("<LFD>").append(ufv.getUfvCalculatedLastFreeDayDate()).append("</LFD>")
                            .append("<LINEPTD>").append(ufv.getUfvPaidThruDay()).append("</LINEPTD>")
                            .append("<LINELFD>").append(ufv.getUfvCalculatedLastFreeDayDate()).append("</LINELFD>")
                            .append("<VESSEL>").append(unit.getInboundCv().getCarrierVehicleId()).append("</VESSEL>")
                            .append("<VOYAGE>").append(unit.getInboundCv().getCarrierIbVoyNbrOrTrainId()).append("</VOYAGE>")
                            .append("<ITEMGKEY>").append(invCharge.getGkey()).append("</ITEMGKEY>")
                            .append("<GTD>").append(ufv.getUfvGuaranteeThruDay()).append("</GTD>")
                            .append("<EQSZ>").append(equipment.getEqEquipType().getEqtypNominalLength().getKey()).append("</EQSZ>")
                            .append("<EQTP>").append(equipment.getEqEquipType().getEqtypIsoGroup().getKey()).append("</EQTP>")
                            .append("<EQHT>").append(equipment.getEqEquipType().getEqtypNominalHeight().getKey()).append("</EQHT>")
                            .append("<OWNER>").append(equipment.getEquipmentOwnerId()).append("</OWNER>")
                            .append("<LINE>").append(unit.getUnitLineOperator().getBzuId()).append("</LINE>")
                            .append("<ARRLOCTYPE>").append(ufv.getArrivalCarrierVehicleType()).append("</ARRLOCTYPE>")
                            .append("<ARRCALLNBR>").append(inCallNbr).append("</ARRCALLNBR>")
                            .append("<INTIME>").append(ufv.getUfvTimeIn()).append("</INTIME>")
                            .append("<OUTTIME>").append(ufv.getUfvTimeOut()).append("</OUTTIME>")
                            .append("<VESSELNAME>").append(vesselName).append("</VESSELNAME>")
                            .append("<FADATE>").append(faDate).append("</FADATE>")
                            .append("</TABLE>")
                }
            }
            return resultBuilder.toString()
        } else if (gvyClassParam.toUpperCase().equals("CREATEINVOICE")) {
            Equipment equipment = Equipment.findEquipment(eqNbrParam)
            UnitFinder unitFinder = (UnitFinder) Roastery.getBean("unitFinder")
            Unit unit = unitFinder.findActiveUnit(complex, equipment)
            UnitFacilityVisit ufv = unit.getUnitActiveUfvNowActive()
            Date proposedPTD = (Date) formatter.parse(ptdParam)
            EdiInvoice inv = queryUnitChargesInv(ufv, proposedPTD, payeeParam)
            List<InvoiceCharge> invCharges = inv.getInvoiceChargeArray()
            for (InvoiceCharge invCharge : invCharges) {
                resultBuilder.append("<TABLE>")
                        .append("<DRAFTNBR>").append(inv.getDraftNumber()).append("</DRAFTNBR>")
                        .append("<CHARGEEVENTTYPE>").append(invCharge.getChargeEventTypeId()).append("</CHARGEEVENTTYPE>")
                        .append("<ITEMGKEY>").append(invCharge.getGkey()).append("</ITEMGKEY>")
                        .append("</TABLE>")
            }
            return resultBuilder.toString()
        } else if (gvyClassParam.toUpperCase().equals("RECORDPAYMENT")) {
            String[] equipArr = eqNbrParam.toUpperCase().split(",")
            for (String equip : equipArr) {
                Equipment equipment = Equipment.findEquipment(equip)
                UnitFinder unitFinder = (UnitFinder) Roastery.getBean("unitFinder")
                Unit unit = unitFinder.findActiveUnit(complex, equipment)
                if (unit == null) {
                    return "Error:  Container is Unknown"
                }
                if (unit.getUnitCategory() != UnitCategoryEnum.IMPORT) {
                    return "Error:  Container is not an Import"
                }
                UnitFacilityVisit ufv = unit.getUnitActiveUfvNowActive()
                Date checkDate
                if (checkDateParam != null) {
                    checkDate = (Date) formatter.parse(checkDateParam)
                }
                if (invItemGkeysParam != "") {
                    String delete = deleteInvoiceItems(draftNbrParam, invItemGkeysParam)
                }
                String response = recordPayment(draftNbrParam, paymentTypeParam, timeNow, bankNameParam, checkNbrParam,
                        checkRtgNbrParam, checkDate, paymentAmtParam, ufv, payeeParam)

                Sql sqlInvItems = Sql.newInstance(urlBilling, userBilling, passwordBilling, driverClassNameBilling)
                sqlInvItems.eachRow("""
                        select ii.extract_gkey, to_char(ii.from_date,'DD-MON-YY') from_date, to_char(ii.to_date,'DD-MON-YY') to_date, ii.quantity, ii.amount, i.draft_nbr
                          from bil_invoices i
                          join bil_invoice_items ii on i.gkey=ii.invoice_gkey
                         where i.draft_nbr = ${draftNbrParam}
                        """) {
                    Guarantee gte = new Guarantee()
                    ScopedBizUnit contractBizUnit = ScopedBizUnit.findScopedBizUnit(payeeParam, BizRoleEnum.MISC);
                    String pKey = it.EXTRACT_GKEY
                    gte.setGnteGuaranteeId(gte.getGuaranteeIdFromSequenceProvide())
                    gte.setGnteGuaranteeType(GuaranteeTypeEnum.PAID)
                    gte.setGnteAppliedToClass(BillingExtractEntityEnum.INV)
                    gte.setGnteAppliedToPrimaryKey(pKey.toLong())
                    gte.setGnteAppliedToNaturalKey(unit.getUnitId())
                    gte.setGnteGuaranteeCustomer(contractBizUnit)
                    if (it.FROM_DATE != null && it.FROM_DATE != "") {
                        Date startDate = (Date) formatter.parse(it.FROM_DATE)
                        gte.setGnteGuaranteeStartDay(startDate)
                    }
                    if (it.TO_DATE != null && it.TO_DATE != "") {
                        Date endDate = (Date) formatter.parse(it.TO_DATE)
                        gte.setGnteGuaranteeEndDay(endDate)
                    }
                    gte.setGnteQuantity(it.QUANTITY)
                    gte.setGnteGuaranteeAmount(it.AMOUNT)
                    gte.setGntePaymentType(PaymentTypeEnum.getEnum(paymentTypeParam))
                    gte.setGnteN4UserId("admin")
                    gte.setGnteInvDraftNbr(it.DRAFT_NBR.toString())
                    HibernateApi.getInstance().saveOrUpdate(gte)
                }
                String cues = releasePaymentHolds(draftNbrParam, ufv, unit)
                return cues
            }
        } else if (gvyClassParam.toUpperCase().equals("DELETEINVOICE")) {
            String delete = deleteInvoice(draftNbrParam)
            return "Invoice Deleted Successfully"
        } else if (gvyClassParam.toUpperCase().equals("RECORDGUARANTEE")) {
            String[] equipArr = eqNbrParam.toUpperCase().split(",")
            String cues = ""
            for (String equip : equipArr) {
                Equipment equipment = Equipment.findEquipment(equip)
                UnitFinder unitFinder = (UnitFinder) Roastery.getBean("unitFinder")
                Unit unit = unitFinder.findActiveUnit(complex, equipment)
                if (unit == null) {
                    return "Error:  Container is Unknown"
                }
                UnitFacilityVisit ufv = unit.getUnitActiveUfvNowActive()
                DomainQuery dq = QueryFactory.createDomainQuery(ArgoExtractEntity.CHARGEABLE_UNIT_EVENT)
                        .addDqPredicate(PredicateFactory.eq(ArgoExtractField.BEXU_EVENT_TYPE, eventTypeParam))
                        .addDqPredicate(PredicateFactory.eq(ArgoExtractField.BEXU_UFV_GKEY, ufv.getUfvGkey()))
                        .addDqPredicate(PredicateFactory.eq(ArgoExtractField.BEXU_UNIT_GKEY, unit.getUnitGkey()))
                List<ChargeableUnitEvent> cueList = HibernateApi.getInstance().findEntitiesByDomainQuery(dq)
                ChargeableUnitEvent cue = cueList.get(0)
                Guarantee gte = new Guarantee()
                ScopedBizUnit contractBizUnit = ScopedBizUnit.findScopedBizUnit(payeeParam, BizRoleEnum.MISC);
                gte.setGnteGuaranteeId(gte.getGuaranteeIdFromSequenceProvide())
                gte.setGnteGuaranteeType(GuaranteeTypeEnum.PRE_PAY)
                gte.setGnteAppliedToClass(BillingExtractEntityEnum.INV)
                gte.setGnteAppliedToPrimaryKey(cue.getBexuGkey())
                gte.setGnteAppliedToNaturalKey(unit.getUnitId())
                gte.setGnteGuaranteeCustomer(contractBizUnit)
                if (startDateParam != null && startDateParam != "") {
                    Date startDate = (Date) formatter.parse(startDateParam)
                    gte.setGnteGuaranteeStartDay(startDate)
                }
                if (endDateParam != null && endDateParam != "") {
                    Date endDate = (Date) formatter.parse(endDateParam)
                    gte.setGnteGuaranteeEndDay(endDate)
                }
                gte.setGnteQuantity(new Double(qtyParam))
                gte.setGnteGuaranteeAmount(new Double(paymentAmtParam))
                gte.setGntePaymentType(PaymentTypeEnum.ON_ACCOUNT)
                gte.setGnteN4UserId("admin")
                gte.setGnteInvDraftNbr(cue.getLastDraftInvNbr())
                gte.setGnteInvoiceStatus(cue.getGnteInvProcessingStatus())
                HibernateApi.getInstance().saveOrUpdate(gte)
                Date[] compDates
                compDates = new Date[3]
                compDates[0] = ufv.getUfvCalculatedLastFreeDayDate()
                compDates[1] = ufv.getUfvPaidThruDay()
                compDates[2] = ufv.getUfvGuaranteeThruDay()
                compDates.sort()
                Date compDate = compDates[2]
                Calendar c = Calendar.getInstance()
                c.setTime(compDate)
                c.add(Calendar.DATE, 1)
                Date dt = c.getTime()
                if (dt.equals(gte.getGnteGuaranteeStartDay())) {
                    ufv.setUfvGuaranteeParty(gte.getGnteGuaranteeCustomer())
                    ufv.setUfvGuaranteeThruDay(gte.getGnteGuaranteeEndDay())
                    HibernateApi.getInstance().saveOrUpdate(ufv)
                }
                cues = cues + releasePaymentHoldsGnte(draftNbrParam, ufv, unit, eventTypeParam)
            }
            return cues
        }
        return results
    }

    public EdiInvoice queryUnitChargesInv(UnitFacilityVisit inUfv, Date inPTD, String inPayee) {

        if (inUfv == null) {
            println("No Unit facility visit is passed");
            return;
        }
        String action = "CREATE";
        String invoiceTypeId = "QUERY_IMPORT_CHARGES_INVOICE_TYPE";
        String currencyId = "USD";
        String unitId = inUfv.getPrimaryEqId();
        Date paidThruDate = inPTD;
        ScopedBizUnit payeeBizUnit = ScopedBizUnit.findScopedBizUnit(inPayee, BizRoleEnum.MISC);
        ScopedBizUnit contractBizUnit = inUfv.getUfvUnit().getUnitLineOperator();
        EdiInvoice ediInvoice;
        Long ufvGkey = inUfv.getUfvGkey();
        try {
            Element element = buildGetInvoiceByInvTypeIdForUnitElement(unitId, invoiceTypeId, action, payeeBizUnit,
                    contractBizUnit, currencyId, null, paidThruDate, null, ufvGkey);
            ediInvoice = getInvoiceByInvTypeIdForUnit(element);
        } catch (Exception e) {
            return;
        }
        if (ediInvoice != null) {
            return ediInvoice
        }
    }

    public String recordPayment(String inDraftNbr, String inPaymentType, Date inDate,
                                String inBankName, String inCheckNbr, String inCheckRtgNbr,
                                Date inCheckDate, String inAmount, UnitFacilityVisit inUfv,
                                String inPayee) {

        if (inUfv == null) {
            println("No Unit facility visit is passed");
            return;
        }
        String currencyId = "USD";
        ScopedBizUnit contractBizUnit = ScopedBizUnit.findScopedBizUnit(inPayee, BizRoleEnum.MISC);
        String response
        PaymentTypeEnum paymentType = PaymentTypeEnum.getEnum(inPaymentType)
        try {
            Element element = buildRecordPaymentRequest(inDraftNbr, paymentType, inDate, inBankName,
                    inCheckNbr, inCheckRtgNbr, inCheckDate, inAmount, currencyId, contractBizUnit);
            response = addPayment(element);
        } catch (Exception e) {
            println("exception in buildRecordPaymentRequest - " + e.toString())
            return;
        }
        return response
    }

    public String deleteInvoice(String inDraftNbr) {
        String response
        try {
            Element element = buildDeleteDraftInvoiceRequest(inDraftNbr);
            response = deleteInvoiceRequest(element)
        } catch (Exception e) {
            response = "Error"
            return response;
        }
        response = "Success"
        return response
    }

    public String deleteInvoiceItems(String inDraftNbr, String inItemGkeys) {
        String response
        List<String> gkeysToRemove = inItemGkeys.split(",")
        for (String itemGkey in gkeysToRemove) {

            try {
                Element element = buildDeleteInvoiceItemRequest(inDraftNbr, itemGkey);
                response = deleteInvoiceRequest(element)
            } catch (Exception e) {
                response = e.toString()
                return response;
            }
        }
        response = "Success"
        return response
    }

    public String releasePaymentHolds(String inDraftNbr, UnitFacilityVisit inUfv, Unit inUnit) {
        String response = ""
        DomainQuery dq = QueryFactory.createDomainQuery(ArgoExtractEntity.CHARGEABLE_UNIT_EVENT)
                .addDqPredicate(PredicateFactory.eq(ArgoExtractField.BEXU_STATUS, "INVOICED"))
                .addDqPredicate(PredicateFactory.eq(ArgoExtractField.BEXU_LAST_DRAFT_INV_NBR, inDraftNbr))
                .addDqPredicate(PredicateFactory.eq(ArgoExtractField.BEXU_UFV_GKEY, inUfv.getUfvGkey()))
                .addDqPredicate(PredicateFactory.eq(ArgoExtractField.BEXU_UNIT_GKEY, inUnit.getUnitGkey()))
                .addDqPredicate(PredicateFactory.ne(ArgoExtractField.BEXU_EVENT_TYPE, "STORAGE"))
        List<ChargeableUnitEvent> cueList = HibernateApi.getInstance().findEntitiesByDomainQuery(dq)
        for (ChargeableUnitEvent cue in cueList) {
            response = response + "<TABLE><GKEY>" + cue.getBexuGkey() + "</GKEY></TABLE>"
        }
        return response
    }

    public String releasePaymentHoldsGnte(String inDraftNbr, UnitFacilityVisit inUfv, Unit inUnit, String inEventType) {
        String response = ""
        DomainQuery dq = QueryFactory.createDomainQuery(ArgoExtractEntity.CHARGEABLE_UNIT_EVENT)
                .addDqPredicate(PredicateFactory.eq(ArgoExtractField.BEXU_EVENT_TYPE, inEventType))
        // .addDqPredicate(PredicateFactory.eq(ArgoExtractField.BEXU_LAST_DRAFT_INV_NBR, inDraftNbr))
                .addDqPredicate(PredicateFactory.eq(ArgoExtractField.BEXU_UFV_GKEY, inUfv.getUfvGkey()))
                .addDqPredicate(PredicateFactory.eq(ArgoExtractField.BEXU_UNIT_GKEY, inUnit.getUnitGkey()))
                .addDqPredicate(PredicateFactory.ne(ArgoExtractField.BEXU_EVENT_TYPE, "STORAGE"))
        List<ChargeableUnitEvent> cueList = HibernateApi.getInstance().findEntitiesByDomainQuery(dq)
        for (ChargeableUnitEvent cue in cueList) {
            response = response + "<TABLE><GKEY>" + cue.getBexuGkey() + "</GKEY></TABLE>"
        }
        return response
    }

    private Element buildDeleteDraftInvoiceRequest(String draftNbr) {
        //build the request xml
        Element rootElem = new Element(BillingWsApiConsts.BILLING_ROOT, XmlUtil.ARGO_NAMESPACE);
        Element elem = new Element(BillingWsApiConsts.DELETE_DRAFT_INVOICE_REQUEST, XmlUtil.ARGO_NAMESPACE);
        rootElem.addContent(elem);
        addChildTextElement(BillingWsApiConsts.DRAFT_INVOICE_NBR, draftNbr, elem);
        return rootElem;
    }

    private Element buildDeleteInvoiceItemRequest(String draftNbr, String itemGkey) {
        //build the request xml
        Element rootElem = new Element(BillingWsApiConsts.BILLING_ROOT, XmlUtil.ARGO_NAMESPACE);
        Element elem = new Element(BillingWsApiConsts.DELETE_DRAFT_INVOICE_REQUEST, XmlUtil.ARGO_NAMESPACE)
        rootElem.addContent(elem);
        addChildTextElement(BillingWsApiConsts.DRAFT_INVOICE_NBR, draftNbr, elem);
        Element paramsElem = new Element(BillingWsApiConsts.INVOICE_ITEM_GKEYS, XmlUtil.ARGO_NAMESPACE);
        addChildTextElement(BillingWsApiConsts.INVOICE_ITEM_GKEY, itemGkey, paramsElem);
        elem.addContent(paramsElem);
        return rootElem;
    }

    public String deleteInvoiceRequest(Element inElement) throws BizViolation {
        try {
            ArgoServicePort port = getWsStub();
            ScopeCoordinateIdsWsType scopeCoordinates = getScopeCoordenatesForWs();
            GenericInvokeResponseWsType invokeResponseWsType = port.genericInvoke(scopeCoordinates, XmlUtil.toString(inElement, false));
            ResponseType response = invokeResponseWsType.getCommonResponse();
            QueryResultType[] queryResultTypes = response.getQueryResults();
            if (queryResultTypes == null || queryResultTypes.length != 1) {
                if (response.getMessageCollector() != null && response.getMessageCollector().getMessages(0) != null) {
                    MessageType type = response.getMessageCollector().getMessages(0);
                    String message = type.getMessage();
                    throw BizFailure.create("Error from Billing Webservice - " + message);
                } else {
                    throw BizFailure.create(ArgoPropertyKeys.BILLING_WEBSERVICE_SERVICES_URL, null, null);
                }
            }
            String responseString = queryResultTypes[0].getResult();

            return responseString;

        } catch (Exception e) {
            println("Error in delete invoice request " + e.toString())
            return;
        }
    }

    private Element buildRecordPaymentRequest(String draftNbr, PaymentTypeEnum paymentType, Date cDate, String bankName,
                                              String checkNbr, String checkRtgNbr, Date checkDate, String amt,
                                              String currId, ScopedBizUnit inCustomer) {
        //build the request xml
        Element rootElem = new Element(BillingWsApiConsts.BILLING_ROOT, XmlUtil.ARGO_NAMESPACE);
        Element elem = new Element(BillingWsApiConsts.RECORD_PAYMENT_REQUEST, XmlUtil.ARGO_NAMESPACE);
        rootElem.addContent(elem);
        addChildTextElement(BillingWsApiConsts.PAYMENT_TYPE, paymentType.getKey(), elem);
        String paymentDateStr = BillingWsApiConsts.XML_DATE_TIME_ZONE_FORMAT.format(cDate);
        addChildTextElement(BillingWsApiConsts.PAYMENT_DATE, paymentDateStr, elem);
        if (bankName != null && bankName != "") {
            addChildTextElement(BillingWsApiConsts.PAYMENT_BANK_NAME, bankName, elem);
        }
        if (checkNbr != null && checkNbr != "") {
            addChildTextElement(BillingWsApiConsts.PAYMENT_CHEQUE_NBR, checkNbr, elem);
        }
        if (checkRtgNbr != null && checkRtgNbr != "") {
            addChildTextElement(BillingWsApiConsts.PAYMENT_CHEQUE_RTTN, checkRtgNbr, elem);
        }
        if (checkDate != null && checkDate != "") {
            String checkDateStr = BillingWsApiConsts.XML_DATE_TIME_ZONE_FORMAT.format(checkDate);
            addChildTextElement(BillingWsApiConsts.PAYMENT_CHEQUE_DATE, checkDateStr, elem);
        }
        addChildTextElement(BillingWsApiConsts.PAYMENT_AMOUNT, amt, elem);
        addChildTextElement(BillingWsApiConsts.PAYMENT_CURRENCY, currId, elem);
        addChildTextElement(BillingWsApiConsts.PAYMENT_CUSTOMER, inCustomer.getBzuId(), elem);
        addChildTextElement(BillingWsApiConsts.PAYMENT_CUSTOMER_BIZ_ROLE, BizRoleEnum.MISC.getKey(), elem);
        Element paramsElem = new Element(BillingWsApiConsts.PAYMENT_ITEMS, XmlUtil.ARGO_NAMESPACE);
        Element paramElem = new Element(BillingWsApiConsts.PAYMENT_ITEM, XmlUtil.ARGO_NAMESPACE);
        addChildTextElement(BillingWsApiConsts.PAYMENT_INVOICE_DRAFT_ID, draftNbr, paramElem);
        addChildTextElement(BillingWsApiConsts.PAYMENT_AMOUNT, amt, paramElem);
        paramsElem.addContent(paramElem);
        elem.addContent(paramsElem);
        return rootElem;
    }

    public String addPayment(Element inElement) throws BizViolation {
        try {
            ArgoServicePort port = getWsStub();
            ScopeCoordinateIdsWsType scopeCoordinates = getScopeCoordenatesForWs();
            GenericInvokeResponseWsType invokeResponseWsType = port.genericInvoke(scopeCoordinates, XmlUtil.toString(inElement, false));
            ResponseType response = invokeResponseWsType.getCommonResponse();
            QueryResultType[] queryResultTypes = response.getQueryResults();
            if (queryResultTypes == null || queryResultTypes.length != 1) {
                if (response.getMessageCollector() != null && response.getMessageCollector().getMessages(0) != null) {
                    MessageType type = response.getMessageCollector().getMessages(0);
                    String message = type.getMessage();
                    throw BizFailure.create("Error from Billing Webservice - " + message);
                } else {
                    throw BizFailure.create(ArgoPropertyKeys.BILLING_WEBSERVICE_SERVICES_URL, null, null);
                }
            }
            String responseString = queryResultTypes[0].getResult();

            return responseString

        } catch (Exception e) {
            println("hit exception in addPayment " + e.toString())
            return;
        }
    }

    private Element buildGetInvoiceByInvTypeIdForUnitElement(String inUnitId, String invoiceTypeId, String inAction, ScopedBizUnit inPayee,
                                                             ScopedBizUnit inContractCustomer, String inCurrencyId, Date inContractEffectiveDate,
                                                             Date inPaidThruDate, Date inStartDate, Long inUfvGkey) {
        //build the request xml
        Element rootElem = new Element(BillingWsApiConsts.BILLING_ROOT, XmlUtil.ARGO_NAMESPACE);
        Element elem = new Element(BillingWsApiConsts.GENERATE_INVOICE_REQUEST, XmlUtil.ARGO_NAMESPACE);
        rootElem.addContent(elem);
        addChildTextElement(BillingWsApiConsts.INVOICE_TYPE_ID, invoiceTypeId, elem);
        addChildTextElement(BillingWsApiConsts.ACTION, inAction, elem);
        addChildTextElement(BillingWsApiConsts.PAYEE_CUSTOMER_ID, inPayee.getBzuId(), elem);
        addChildTextElement(BillingWsApiConsts.PAYEE_CUSTOMER_BIZ_ROLE, inPayee.getBzuRole().getKey(), elem);
        String contractCustId = inContractCustomer != null ? inContractCustomer.getBzuId() : "";
        addChildTextElement(BillingWsApiConsts.CONTRACT_CUSTOMER_ID, contractCustId, elem);
        addChildTextElement(BillingWsApiConsts.CONTRACT_CUSTOMER_BIZ_ROLE, inContractCustomer != null ? inContractCustomer.getBzuRole().getKey() : null, elem);
        addChildTextElement(BillingWsApiConsts.CURRENCY_ID, inCurrencyId, elem);
        String effectiveDateStr = null;
        if (inContractEffectiveDate != null) {
            effectiveDateStr = BillingWsApiConsts.XML_DATE_TIME_ZONE_FORMAT.format(inContractEffectiveDate);
        }
        addChildTextElement(BillingWsApiConsts.CONTRACT_EFFECTIVE_DATE, effectiveDateStr, elem);
        addChildTextElement(BillingWsApiConsts.IS_INVOICE_FINAL, "false", elem);
        Element paramsElem = new Element(BillingWsApiConsts.INVOICE_PARAMETERS, XmlUtil.ARGO_NAMESPACE);
        Element paramElem = new Element(BillingWsApiConsts.INVOICE_PARAMETER, XmlUtil.ARGO_NAMESPACE);

        // invoice parameters
        String paidThruDayStr = null;
        if (inPaidThruDate != null) {
            paidThruDayStr = BillingWsApiConsts.XML_DATE_TIME_ZONE_FORMAT.format(inPaidThruDate);
        }
        addChildTextElement(BillingWsApiConsts.PAID_THRU_DAY, paidThruDayStr, paramElem);
        addChildTextElement(BillingWsApiConsts.EQUIPMENT_ID, inUnitId, paramElem);
        addChildTextElement(BillingWsApiConsts.UFV_GKEY, inUfvGkey.toString(), paramElem)
        paramsElem.addContent(paramElem);
        elem.addContent(paramsElem);
        return rootElem;
    }

    private void addChildTextElement(String inElementName, String inElementText, Element inParentElement) {
        Element childElement = new Element(inElementName, XmlUtil.ARGO_NAMESPACE);
        Text childText = new Text(inElementText);
        childElement.addContent(childText);
        inParentElement.addContent(childElement);
    }

    public EdiInvoice getInvoiceByInvTypeIdForUnit(Element inElement) throws BizViolation {

        try {
            ArgoServicePort port = getWsStub();
            ScopeCoordinateIdsWsType scopeCoordinates = getScopeCoordenatesForWs();
            GenericInvokeResponseWsType invokeResponseWsType = port.genericInvoke(scopeCoordinates, XmlUtil.toString(inElement, false));
            ResponseType response = invokeResponseWsType.getCommonResponse();
            QueryResultType[] queryResultTypes = response.getQueryResults();
            if (queryResultTypes == null || queryResultTypes.length != 1) {
                if (response.getMessageCollector() != null && response.getMessageCollector().getMessages(0) != null) {
                    MessageType type = response.getMessageCollector().getMessages(0);
                    String message = type.getMessage();
                    throw BizFailure.create("Error from Billing Webservice - " + message);
                } else {
                    throw BizFailure.create(ArgoPropertyKeys.BILLING_WEBSERVICE_SERVICES_URL, null, null);
                }
            }
            String responseString = queryResultTypes[0].getResult();

            BillingTransactionsDocument billingTransactionsDocument = BillingTransactionsDocument.Factory.parse(responseString);
            BillingTransactionsDocument.BillingTransactions transactions = billingTransactionsDocument.getBillingTransactions();
            List<BillingTransactionDocument.BillingTransaction> transactionList = transactions.getBillingTransactionList();
            BillingTransactionDocument.BillingTransaction billingTransaction = transactionList.get(0);
            List<EdiInvoice> list = billingTransaction.getInvoiceList();
            if (list.isEmpty()) {
                throw BizFailure.create(InventoryPropertyKeys.NO_INVOICE_RETRIEVED, null, null);
            } else if (list.size() > 1) {
                throw BizFailure.create(InventoryPropertyKeys.MULTIPLE_INVOICES_RETURNED, null, list.size());
            }
            EdiInvoice ediInvoice = list.get(0);

            return ediInvoice;
        } catch (ServiceException e) {
            throw BizFailure.create(InventoryPropertyKeys.BILLING_WEBSERVICE_ERROR, e, null);
        } catch (java.rmi.RemoteException e) {
            throw BizFailure.create(InventoryPropertyKeys.BILLING_WEBSERVICE_ERROR, e, null);
        } catch (IOException e) {
            throw BizFailure.create(InventoryPropertyKeys.BILLING_WEBSERVICE_XML_ERROR, e, null);
        } catch (org.apache.xmlbeans.XmlException e) {
            throw BizFailure.create(InventoryPropertyKeys.BILLING_WEBSERVICE_XML_ERROR, e, null);
        }
    }

    private ArgoServicePort getWsStub() throws ServiceException {
        ArgoServiceLocator locator = new ArgoServiceLocator();
        ArgoServicePort port = locator.getArgoServicePort(ConfigurationProperties.getBillingServiceURL());
        Stub stub = (Stub) port;
        stub._setProperty(Stub.USERNAME_PROPERTY, ConfigurationProperties.getBillingWebServiceUserId());
        stub._setProperty(Stub.PASSWORD_PROPERTY, ConfigurationProperties.getBillingWebServicePassWord());
        return port;
    }

    private ScopeCoordinateIdsWsType getScopeCoordenatesForWs() {
        //build the scope coordinates for the web service based on the user context;
        ScopeCoordinateIdsWsType scopeCoordinates = new ScopeCoordinateIdsWsType();
        scopeCoordinates.setOperatorId(ContextHelper.getThreadOperator() != null ? ContextHelper.getThreadOperator().getId() : null);
        scopeCoordinates.setComplexId(ContextHelper.getThreadComplex() != null ? ContextHelper.getThreadComplex().getCpxId() : null);
        scopeCoordinates.setFacilityId(ContextHelper.getThreadFacility() != null ? ContextHelper.getThreadFacility().getFcyId() : null);
        scopeCoordinates.setYardId(ContextHelper.getThreadYard() != null ? ContextHelper.getThreadYard().getYrdId() : null);
        return scopeCoordinates;
    }
}

public class ChargesComparator implements Comparator<InvoiceCharge> {
    public int compare(InvoiceCharge i1, InvoiceCharge i2) {
        return i1.getEventPerformedFrom().compareTo(i2.getEventPerformedFrom())
    }
}
