import os
import json
import requests
import time
import logging
import traceback
from datetime import datetime

import settings
from db_connect import MsDbConn
from action_data import ActionData
from processor import Processor


class ERPProcess(Processor):

    def __init__(self):
        super(ERPProcess, self).__init__(interval=7200)
        self.db = None # MsDbConn(settings.ERP_HOST, settings.ERP_USERNAME, settings.ERP_PASSWORD, settings.ERP_DATABASE)
        self.format_datetime = "%Y-%m-%d %H:%M:%S"
        self.pid = '00000'

    def pack_data(self, table_name, out_data, action_m=ActionData.ActionAll):
        if not out_data:
            return None
        d = ActionData(table_name, out_data, action_m, self.pid)
        return d.packet_action()

    def dump_erp_order(self):
        table_name = 'erp_order'
        sql = '''
           SELECT 
           a.sOrderNo,		--订单号
           a.sCustomerOrderNo,--客户订单号
           a.sOrderType,	--订单类型（R：染色订单，RH：外退回修订单，RN：内部回修订单）
           a.dReleaseDate,	--下单日期
           f.sCustomerNo,	--客户编号
           f.sCustomerName,--客户名称
           g.sSalesName,	--业务员（销售员）
           d.sMaterialNo,	--物料编号
           d.sMaterialName,--物料名称
           e.sColorNo,		--色号
           e.sColorName,	--颜色
           b.sProductWidth,--成品门幅
           b.sProductGMWT,	--成品克重
           c.nQty			--数量
           FROM dbo.sdOrderHdr a WITH(NOLOCK)
           JOIN dbo.sdOrderDtl b WITH(NOLOCK) ON b.usdOrderHdrGUID = a.uGUID
           JOIN dbo.sdOrderLot c WITH(NOLOCK) ON c.usdOrderDtlGUID = b.uGUID
           JOIN dbo.vwmmMaterialFabric d WITH(NOLOCK) ON d.uGUID = b.ummMaterialGUID
           JOIN dbo.tmColor e WITH(NOLOCK) ON e.uGUID = b.utmColorGUID
           JOIN dbo.pbCustomer f WITH(NOLOCK) ON f.uGUID = a.upbCustomerGUID
           JOIN dbo.pbSales g WITH(NOLOCK) ON g.uGUID = a.upbSalesGUID
            '''
        data_list = self.db.query_all(sql)
        out_data = []
        for data in data_list:
            data = list(data)
            if isinstance(data[3], datetime):
                data[3] = data[3].strftime("%Y-%m-%d %H:%M:%S")
            # else:
            #     print(data)
            data[13] = float(data[13])
            out_data.append(data)
        return self.pack_data(table_name, out_data)

    def dump_erp_m_card_order(self):
        table_name = 'erp_m_card_order'
        sql = '''
        SELECT 
        a.sCardNo,			--卡号
        a.sRawFabricNo,     --缸数
        a.sLinkType,		--卡类型（NULL、分卡、回修）
        a.nPlanOutputQtyEx,	--匹数
        a.nPlanOutputQty,	--数量
        b.sOrderNo		--订单表相关字段
        FROM dbo.psWorkFlowCard a WITH(NOLOCK)
        JOIN dbo.vwsdOrder b WITH(NOLOCK) ON b.usdOrderLotGUID=a.usdOrderLotGUID  --订单视图
        '''
        data_list = self.db.query_all(sql)
        out_data = []
        for data in data_list:
            # print(data)
            data = list(data)
            data[3] = float(data[3])
            data[4] = float(data[4])
            out_data.append(data)
        return self.pack_data(table_name, out_data)

    def dump_erp_store(self):
        table_name = 'erp_store'
        sql = '''
           SELECT 
           a.sStoreInNo,		--入库单号
           a.tStoreInTime,		--入库时间
           a.immStoreInTypeID,	--入库类型（3：车间生产入库）
           a.iStoreInStatus,	--入库状态（0：未审核，1：审核）
           b.sCardNo,			--流程卡号
           b.iOrderNo,			--序号
           b.nInQty,			--数量
           b.sProductWidth,	--成品门幅
           b.sProductGMWT		--成品克重
           FROM dbo.mmSTInHdr a WITH(NOLOCK)
           JOIN dbo.mmSTInDtl b WITH(NOLOCK) ON b.ummInHdrGUID = a.uGUID
           JOIN dbo.mmStore c WITH(NOLOCK) ON c.uGUID = a.ummStoreGUID
           '''
        data_list = self.db.query_all(sql)
        out_data = []
        for data in data_list:
            # print(data)
            data = list(data)
            if isinstance(data[1], datetime):
                data[1] = data[1].strftime("%Y-%m-%d %H:%M:%S")
            data[6] = float(data[6])
            out_data.append(data)
        return self.pack_data(table_name, out_data)

    def dump_erp_wuhao(self):
        table_name = 'erp_wuhao'
        sql = '''
            SELECT 
            B.tStoreOutTime,	--出库时间
            B.sStoreOutNo,		--出库单号
            D.sMaterialNo,		--物料编号
            D.sMaterialName,	--物料名称
            E.sMaterialTypeName,--物料类型
            A.nStoreOutQty,		--出库数量
            D.sUnit,			--出库单位
            A.nACPrice,			--单位
            A.nAmount,			--金额
            B.immStoreOutTypeID,--出库类型(3:领用, 2:退货)
            M.sStoreName,		--仓库
            B.ummStoreGUID     --UUID
            FROM dbo.mmDCOutDtl A
            LEFT JOIN dbo.mmDCOutHdr B ON B.uGUID = A.ummOutHdrGUID
            LEFT JOIN dbo.vwmmDCStoreCheckItem C ON C.uGUID = B.uRefDestGUID
            LEFT JOIN dbo.mmDCInDtl D ON D.uGUID = A.ummInDtlGUID
            LEFT JOIN dbo.vwmmMaterialChemical E ON E.uGUID = D.ummMaterialGUID
            LEFT JOIN dbo.mmStore M ON M.uGUID = B.ummStoreGUID
            WHERE B.iStoreOutStatus = 1
           '''
        data_list = self.db.query_all(sql)
        out_data = []
        for data in data_list:
            data = list(data)
            data[0] = data[0].strftime("%Y-%m-%d %H:%M:%S")
            data[5] = float(data[5])
            data[7] = float(data[7])
            data[8] = float(data[8])
            data[11] = str(data[11])
            out_data.append(data)
        return self.pack_data(table_name, out_data)

    def dump_erp_chufangdan(self):
        table_name = 'erp_chufangdan'
        sql = '''
        SELECT 
        b.sPrescriptionNo,	--处方单号
        b.tConfirmTime,		--审核时间
        b.sPrescriptionType,--处方类型（正常、加料）
        a.sCardNo,			--卡号
        d.sMaterialNo,		--物料编号
        d.sMaterialName,	--物料名称
        c.nQty				--用量（单位：G）
        FROM dbo.tmPrescriptionUsage a WITH(NOLOCK)
        JOIN dbo.tmPrescriptionHdr b WITH(NOLOCK) ON b.uGUID = a.utmPrescriptionHdrGUID
        JOIN dbo.tmPrescriptionDtl c WITH(NOLOCK) ON c.utmPrescriptionHdrGUID = b.uGUID
        JOIN dbo.vwmmMaterialChemical d WITH(NOLOCK) ON c.uGUID = b.ummMaterialGUID
        JOIN dbo.pbWorkingProcedure e WITH(NOLOCK) ON e.uGUID = b.upbWorkingProcedureGUID
        WHERE b.tConfirmTime IS NOT NULL
        -- AND e.sWorkingProcedureNo='010'  --染色
        --e.sWorkingProcedureNo='028'  --前处理
        '''
        data_list = self.db.query_all(sql)
        out_data = []
        for data in data_list:
            data = list(data)
            out_data.append(data)
            # print(data)
        return self.pack_data(table_name, out_data)

    def collect_data(self):
        content = []
        function_list = [
            self.dump_erp_order,
            self.dump_erp_m_card_order,
            self.dump_erp_store,
            self.dump_erp_wuhao,
            # self.dump_erp_chufangdan(),
        ]
        for dump_function in function_list:
            try:
                out_data = dump_function()
            except Exception as e:
                logging.getLogger('agent').debug(traceback.format_exc())
                logging.getLogger('agent').error("process function %s, %s" % (e, dump_function))
            else:
                if not out_data:
                    logging.getLogger('agent').debug("out_data[%s] is none" % dump_function)
                    continue
                logging.getLogger('agent').debug("out_data: %s,  %s" % (dump_function, out_data))
                content.append(out_data)

        return {'ts': int(time.time()), 'data': content}

    def parse_response(self, content):
        return True

    def post_data(self, data, tries=3):
        for i in range(1, tries+1):
            try:
                req = requests.post(settings.DB_ERP_URL, json=data)
            except Exception as err:
                logging.getLogger('agent').error('post erp_data error, %s, try num %i' % (err, i))
            else:
                content = req.content
                self.parse_response(req.content)
                logging.getLogger('agent').debug('post erp data, back data %s' % req.content)
                break
        else:
            raise Exception("has try %s nums, transfer maybe die, post erp_data error" % tries)

    def process(self):
        logging.getLogger('agent').info("start sync erp data ...")
        self.db = MsDbConn(settings.ERP_HOST, settings.ERP_USERNAME, settings.ERP_PASSWORD, settings.ERP_DATABASE)
        data = self.collect_data()
        self.post_data(data)
        self.db.close()
        # logging.getLogger('agent').debug("post %s" % post_data)
        logging.getLogger('agent').info('over sync erp data...')
