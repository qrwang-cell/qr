#!/usr/bin/env python3
"""
供应链异常预警与自动处置 Agent MVP
Supply Chain Anomaly Detection & Auto-Resolution Agent

核心架构：
1. Monitoring Agent: 多源数据采集与异常检测
2. Diagnosis Agent: 长链推理根因分析
3. Decision Agent: 多方案评估与决策
4. Execution Agent: 自动执行与闭环反馈
"""

import json
import random
import time
import threading
from datetime import datetime, timedelta
from dataclasses import dataclass, field, asdict
from typing import List, Dict, Optional, Callable, Any
from enum import Enum
from collections import deque
import heapq

# ==================== 数据模型 ====================

class AnomalyLevel(Enum):
    NORMAL = "normal"
    WARNING = "warning"      # 黄色预警
    CRITICAL = "critical"    # 红色预警
    EMERGENCY = "emergency"  # 紧急，自动执行

class AnomalyType(Enum):
    DELAY = "delay"              # 物流延迟
    STOCKOUT = "stockout"        # 库存断货风险
    OVERSTOCK = "overstock"      # 库存积压
    QUALITY = "quality"          # 质量问题
    DEMAND_SURGE = "demand_surge" # 需求激增
    SUPPLIER_RISK = "supplier_risk" # 供应商风险

class ActionType(Enum):
    TRANSFER = "transfer"        # 调拨
    URGENT_PURCHASE = "urgent_purchase"  # 紧急采购
    PREORDER_SWITCH = "preorder_switch"   # 切换预售
    DISCOUNT_CLEAR = "discount_clear"     # 降价清仓
    SUPPLIER_SWITCH = "supplier_switch" # 切换供应商
    MANUAL_REVIEW = "manual_review"     # 人工审核

@dataclass
class Product:
    id: str
    name: str
    sku: str
    category: str
    supplier_id: str
    warehouse_id: str
    current_stock: int
    safety_stock: int
    reorder_point: int
    lead_time_days: int  # 采购提前期
    daily_sales_avg: float
    unit_cost: float
    selling_price: float

@dataclass
class Warehouse:
    id: str
    name: str
    location: str
    capacity: int
    current_load: int

@dataclass
class Supplier:
    id: str
    name: str
    reliability_score: float  # 0-1
    avg_lead_time: int
    is_active: bool = True

@dataclass
class Order:
    id: str
    product_id: str
    quantity: int
    status: str  # pending, shipped, delivered, delayed
    created_at: datetime
    estimated_delivery: datetime
    actual_delivery: Optional[datetime] = None
    carrier: str = ""
    tracking_number: str = ""

@dataclass
class SalesRecord:
    product_id: str
    quantity: int
    timestamp: datetime
    channel: str  # online, offline, wholesale

@dataclass
class ExternalEvent:
    event_type: str  # weather, holiday, news, policy
    description: str
    affected_regions: List[str]
    severity: float  # 0-1
    timestamp: datetime

@dataclass
class AnomalySignal:
    id: str
    timestamp: datetime
    anomaly_type: AnomalyType
    level: AnomalyLevel
    product_id: str
    warehouse_id: str
    description: str
    metrics: Dict[str, Any]
    related_orders: List[str] = field(default_factory=list)
    external_factors: List[str] = field(default_factory=list)
    confidence: float = 0.0  # 异常置信度

@dataclass
class RootCause:
    primary_cause: str
    causal_chain: List[str]  # 长链推理路径
    evidence: List[str]
    confidence: float

@dataclass
class ResolutionPlan:
    plan_id: str
    action_type: ActionType
    description: str
    estimated_cost: float
    estimated_time_hours: float
    success_probability: float
    affected_products: List[str]
    required_approvals: List[str]
    auto_executable: bool  # 是否可自动执行

@dataclass
class ExecutionResult:
    plan_id: str
    executed_at: datetime
    status: str  # success, partial, failed, pending_approval
    details: Dict[str, Any]
    cost_incurred: float
    time_taken_minutes: float
    follow_up_required: bool

# ==================== 模拟数据层 ====================

class DataSimulator:
    """模拟 ERP、WMS、物流系统的数据源"""
    
    def __init__(self):
        self.products: Dict[str, Product] = {}
        self.warehouses: Dict[str, Warehouse] = {}
        self.suppliers: Dict[str, Supplier] = {}
        self.orders: Dict[str, Order] = {}
        self.sales_history: deque = deque(maxlen=10000)
        self.external_events: List[ExternalEvent] = []
        self._init_mock_data()
        self._running = False
        self._thread: Optional[threading.Thread] = None
        
    def _init_mock_data(self):
        # 初始化仓库
        self.warehouses = {
            "WH-BJ": Warehouse("WH-BJ", "北京中心仓", "北京", 50000, 35000),
            "WH-SH": Warehouse("WH-SH", "上海中心仓", "上海", 60000, 42000),
            "WH-GZ": Warehouse("WH-GZ", "广州中心仓", "广州", 45000, 30000),
            "WH-CD": Warehouse("WH-CD", "成都中心仓", "成都", 30000, 18000),
        }
        
        # 初始化供应商
        self.suppliers = {
            "SUP-001": Supplier("SUP-001", "华东电子集团", 0.92, 5),
            "SUP-002": Supplier("SUP-002", "南方制造厂", 0.78, 7),
            "SUP-003": Supplier("SUP-003", "西部供应商", 0.85, 10),
            "SUP-004": Supplier("SUP-004", "国际供应商A", 0.70, 15),
        }
        
        # 初始化商品（模拟3C数码产品）
        products_data = [
            ("P-1001", "无线蓝牙耳机 Pro", "BT-PRO-001", "音频", "SUP-001", "WH-BJ", 850, 200, 300, 5, 45, 120, 299),
            ("P-1002", "智能手表 Series 5", "SW-S5-002", "穿戴", "SUP-001", "WH-SH", 420, 150, 250, 5, 38, 800, 1999),
            ("P-1003", "机械键盘 RGB", "KB-RGB-003", "外设", "SUP-002", "WH-GZ", 120, 100, 180, 7, 25, 200, 499),
            ("P-1004", "4K 显示器 27寸", "MON-4K-004", "显示", "SUP-002", "WH-BJ", 65, 50, 80, 7, 12, 1500, 3299),
            ("P-1005", "氮化镓充电器 65W", "CHG-65W-005", "配件", "SUP-003", "WH-CD", 2000, 300, 500, 10, 80, 80, 199),
            ("P-1006", "平板电脑 Air", "TAB-AIR-006", "平板", "SUP-001", "WH-SH", 180, 80, 120, 5, 22, 2500, 4999),
            ("P-1007", "降噪耳机 Max", "HP-MAX-007", "音频", "SUP-004", "WH-GZ", 95, 60, 100, 15, 15, 1800, 3999),
            ("P-1008", "智能手环 Lite", "BD-LITE-008", "穿戴", "SUP-003", "WH-CD", 600, 150, 250, 10, 40, 150, 399),
        ]
        
        for d in products_data:
            p = Product(*d)
            self.products[p.id] = p
            
        # 初始化在途订单
        for i in range(50):
            pid = random.choice(list(self.products.keys()))
            product = self.products[pid]
            order = Order(
                id=f"ORD-{2024050000 + i}",
                product_id=pid,
                quantity=random.randint(10, 200),
                status=random.choice(["pending", "shipped", "in_transit"]),
                created_at=datetime.now() - timedelta(days=random.randint(1, 10)),
                estimated_delivery=datetime.now() + timedelta(days=random.randint(1, 20)),
                carrier=random.choice(["顺丰", "京东物流", "中通", "圆通"]),
                tracking_number=f"SF{random.randint(100000000, 999999999)}"
            )
            self.orders[order.id] = order
            
        # 初始化历史销售数据
        for _ in range(1000):
            self.sales_history.append(SalesRecord(
                product_id=random.choice(list(self.products.keys())),
                quantity=random.randint(1, 10),
                timestamp=datetime.now() - timedelta(days=random.randint(0, 30)),
                channel=random.choice(["online", "offline", "wholesale"])
            ))
            
    def start_simulation(self):
        """启动后台数据模拟线程"""
        self._running = True
        self._thread = threading.Thread(target=self._simulation_loop, daemon=True)
        self._thread.start()
        print("[DataSimulator] 实时数据模拟已启动")
        
    def stop_simulation(self):
        self._running = False
        if self._thread:
            self._thread.join(timeout=2)
            
    def _simulation_loop(self):
        """模拟真实业务数据变化"""
        while self._running:
            # 模拟销售
            if random.random() < 0.3:
                pid = random.choice(list(self.products.keys()))
                qty = random.randint(1, 5)
                self.products[pid].current_stock = max(0, self.products[pid].current_stock - qty)
                self.sales_history.append(SalesRecord(
                    product_id=pid, quantity=qty, 
                    timestamp=datetime.now(), channel="online"
                ))
                
            # 模拟入库
            if random.random() < 0.1:
                pid = random.choice(list(self.products.keys()))
                qty = random.randint(50, 200)
                self.products[pid].current_stock += qty
                
            # 模拟订单状态更新
            if random.random() < 0.2:
                order = random.choice(list(self.orders.values()))
                if order.status == "pending" and random.random() < 0.7:
                    order.status = "shipped"
                elif order.status == "shipped" and random.random() < 0.5:
                    order.status = "delayed"  # 模拟延迟
                    
            # 模拟外部事件（如天气）
            if random.random() < 0.05:
                event = ExternalEvent(
                    event_type="weather",
                    description=random.choice([
                        "华南地区暴雨红色预警",
                        "华东地区台风登陆",
                        "华北地区大雾封路",
                        "西南地区地震影响交通"
                    ]),
                    affected_regions=random.choice([
                        ["广东", "广西", "福建"],
                        ["上海", "浙江", "江苏"],
                        ["北京", "天津", "河北"],
                        ["四川", "云南", "贵州"]
                    ]),
                    severity=random.uniform(0.5, 1.0),
                    timestamp=datetime.now()
                )
                self.external_events.append(event)
                # 只保留最近20个事件
                self.external_events = self.external_events[-20:]
                
            time.sleep(1)  # 每秒模拟一次业务变化
            
    def get_current_state(self) -> Dict:
        """获取当前全量数据快照"""
        return {
            "products": {k: asdict(v) for k, v in self.products.items()},
            "warehouses": {k: asdict(v) for k, v in self.warehouses.items()},
            "suppliers": {k: asdict(v) for k, v in self.suppliers.items()},
            "orders": {k: asdict(v) for k, v in self.orders.items()},
            "sales_recent": [asdict(s) for s in list(self.sales_history)[-100:]],
            "external_events": [asdict(e) for e in self.external_events[-5:]]
        }

# ==================== Agent 1: 监控 Agent ====================

class MonitoringAgent:
    """
    监控 Agent: 多源数据采集与异常检测
    - 实时扫描库存、订单、销售、外部事件
    - 使用多维度阈值+趋势分析检测异常
    """
    
    def __init__(self, data_simulator: DataSimulator):
        self.ds = data_simulator
        self.anomaly_history: deque = deque(maxlen=1000)
        self._callbacks: List[Callable[[AnomalySignal], None]] = []
        self._running = False
        
    def register_callback(self, callback: Callable[[AnomalySignal], None]):
        self._callbacks.append(callback)
        
    def start_monitoring(self):
        self._running = True
        threading.Thread(target=self._monitor_loop, daemon=True).start()
        print("[MonitoringAgent] 异常监控已启动")
        
    def _monitor_loop(self):
        while self._running:
            signals = self._scan_for_anomalies()
            for signal in signals:
                self.anomaly_history.append(signal)
                for cb in self._callbacks:
                    try:
                        cb(signal)
                    except Exception as e:
                        print(f"[MonitoringAgent] 回调错误: {e}")
            time.sleep(3)  # 每3秒扫描一次
            
    def _scan_for_anomalies(self) -> List[AnomalySignal]:
        signals = []
        now = datetime.now()
        
        for pid, product in self.ds.products.items():
            # 1. 库存断货风险检测
            days_of_stock = product.current_stock / max(product.daily_sales_avg, 1)
            incoming_qty = sum(
                o.quantity for o in self.ds.orders.values() 
                if o.product_id == pid and o.status in ["pending", "shipped"]
            )
            available_days = (product.current_stock + incoming_qty) / max(product.daily_sales_avg, 1)
            
            if available_days < 3 and product.current_stock < product.safety_stock:
                level = AnomalyLevel.EMERGENCY if available_days < 1 else AnomalyLevel.CRITICAL
                signals.append(AnomalySignal(
                    id=f"ANO-{now.strftime('%Y%m%d%H%M%S')}-{pid}",
                    timestamp=now,
                    anomaly_type=AnomalyType.STOCKOUT,
                    level=level,
                    product_id=pid,
                    warehouse_id=product.warehouse_id,
                    description=f"{product.name} 库存告急，预计 {available_days:.1f} 天后断货",
                    metrics={
                        "current_stock": product.current_stock,
                        "safety_stock": product.safety_stock,
                        "days_of_stock": days_of_stock,
                        "available_days": available_days,
                        "incoming_qty": incoming_qty
                    },
                    related_orders=[o.id for o in self.ds.orders.values() if o.product_id == pid],
                    confidence=0.95
                ))
                
            # 2. 库存积压检测
            if days_of_stock > 60 and product.current_stock > product.reorder_point * 3:
                signals.append(AnomalySignal(
                    id=f"ANO-{now.strftime('%Y%m%d%H%M%S')}-{pid}-OVER",
                    timestamp=now,
                    anomaly_type=AnomalyType.OVERSTOCK,
                    level=AnomalyLevel.WARNING,
                    product_id=pid,
                    warehouse_id=product.warehouse_id,
                    description=f"{product.name} 库存积压严重，周转天数 {days_of_stock:.0f} 天",
                    metrics={
                        "current_stock": product.current_stock,
                        "days_of_stock": days_of_stock,
                        "reorder_point": product.reorder_point
                    },
                    confidence=0.85
                ))
                
            # 3. 需求激增检测（对比7日均值）
            recent_sales = [
                s.quantity for s in self.ds.sales_history 
                if s.product_id == pid and s.timestamp > now - timedelta(days=7)
            ]
            if len(recent_sales) >= 3:
                avg_7d = sum(recent_sales) / 7
                if avg_7d > product.daily_sales_avg * 2:
                    signals.append(AnomalySignal(
                        id=f"ANO-{now.strftime('%Y%m%d%H%M%S')}-{pid}-SURGE",
                        timestamp=now,
                        anomaly_type=AnomalyType.DEMAND_SURGE,
                        level=AnomalyLevel.CRITICAL,
                        product_id=pid,
                        warehouse_id=product.warehouse_id,
                        description=f"{product.name} 需求激增，7日均销 {avg_7d:.1f}，超均值 {avg_7d/product.daily_sales_avg:.1f} 倍",
                        metrics={
                            "avg_7d_sales": avg_7d,
                            "baseline": product.daily_sales_avg,
                            "multiplier": avg_7d / product.daily_sales_avg
                        },
                        confidence=0.88
                    ))
                    
        # 4. 物流延迟检测
        for oid, order in self.ds.orders.items():
            if order.status == "delayed" and order.estimated_delivery < now:
                product = self.ds.products.get(order.product_id)
                if product:
                    # 检查是否受外部事件影响
                    affected_events = [
                        e.description for e in self.ds.external_events
                        if any(r in product.warehouse_id or r in order.carrier for r in e.affected_regions)
                    ]
                    signals.append(AnomalySignal(
                        id=f"ANO-{now.strftime('%Y%m%d%H%M%S')}-{oid}",
                        timestamp=now,
                        anomaly_type=AnomalyType.DELAY,
                        level=AnomalyLevel.WARNING,
                        product_id=order.product_id,
                        warehouse_id=product.warehouse_id,
                        description=f"订单 {oid} 物流延迟，已超预计送达时间",
                        metrics={
                            "order_id": oid,
                            "delay_hours": (now - order.estimated_delivery).total_seconds() / 3600,
                            "carrier": order.carrier
                        },
                        related_orders=[oid],
                        external_factors=affected_events,
                        confidence=0.90
                    ))
                    
        # 5. 供应商风险检测
        for sid, supplier in self.ds.suppliers.items():
            if not supplier.is_active:
                affected_products = [
                    p.id for p in self.ds.products.values() if p.supplier_id == sid
                ]
                if affected_products:
                    signals.append(AnomalySignal(
                        id=f"ANO-{now.strftime('%Y%m%d%H%M%S')}-{sid}",
                        timestamp=now,
                        anomaly_type=AnomalyType.SUPPLIER_RISK,
                        level=AnomalyLevel.CRITICAL,
                        product_id=affected_products[0],
                        warehouse_id="",
                        description=f"供应商 {supplier.name} 合作中断，影响 {len(affected_products)} 个 SKU",
                        metrics={
                            "supplier_id": sid,
                            "affected_count": len(affected_products),
                            "affected_products": affected_products
                        },
                        confidence=0.92
                    ))
                    
        return signals

# ==================== Agent 2: 诊断 Agent ====================

class DiagnosisAgent:
    """
    诊断 Agent: 长链推理根因分析
    模拟 Chain-of-Thought 推理过程：
    表面现象 → 直接原因 → 深层原因 → 根因
    """
    
    def __init__(self, data_simulator: DataSimulator):
        self.ds = data_simulator
        
    def diagnose(self, signal: AnomalySignal) -> RootCause:
        """执行长链推理诊断"""
        causal_chain = []
        evidence = []
        
        if signal.anomaly_type == AnomalyType.STOCKOUT:
            causal_chain = self._diagnose_stockout(signal)
        elif signal.anomaly_type == AnomalyType.DELAY:
            causal_chain = self._diagnose_delay(signal)
        elif signal.anomaly_type == AnomalyType.DEMAND_SURGE:
            causal_chain = self._diagnose_demand_surge(signal)
        elif signal.anomaly_type == AnomalyType.OVERSTOCK:
            causal_chain = self._diagnose_overstock(signal)
        elif signal.anomaly_type == AnomalyType.SUPPLIER_RISK:
            causal_chain = self._diagnose_supplier_risk(signal)
        else:
            causal_chain = ["未知异常类型", "需人工介入调查"]
            
        # 收集证据
        evidence = self._gather_evidence(signal, causal_chain)
        
        return RootCause(
            primary_cause=causal_chain[-1] if causal_chain else "未知",
            causal_chain=causal_chain,
            evidence=evidence,
            confidence=signal.confidence * 0.95
        )
        
    def _diagnose_stockout(self, signal: AnomalySignal) -> List[str]:
        """库存断货长链推理"""
        product = self.ds.products.get(signal.product_id)
        chain = []
        
        # Layer 1: 表面现象
        chain.append(f"现象：{product.name} 库存仅剩 {product.current_stock} 件")
        
        # Layer 2: 直接原因
        if product.current_stock < product.safety_stock:
            chain.append(f"直接原因：库存低于安全线（{product.safety_stock}），触发预警")
            
        # Layer 3: 供需分析
        recent_orders = [
            o for o in self.ds.orders.values() 
            if o.product_id == signal.product_id and o.status in ["pending", "shipped"]
        ]
        if not recent_orders:
            chain.append("深层原因：在途订单为零，补货断档")
        else:
            total_incoming = sum(o.quantity for o in recent_orders)
            chain.append(f"深层原因：在途补货 {total_incoming} 件，但日均消耗 {product.daily_sales_avg:.0f} 件，不足以覆盖")
            
        # Layer 4: 根因推断
        if product.lead_time_days > 7:
            chain.append(f"根因：供应商 {self.ds.suppliers[product.supplier_id].name} 交货周期长（{product.lead_time_days}天），响应滞后")
        elif len(recent_orders) == 0:
            chain.append("根因：采购计划失效，未及时下达采购订单")
        else:
            chain.append("根因：需求预测偏差，实际销量远超预期")
            
        return chain
        
    def _diagnose_delay(self, signal: AnomalySignal) -> List[str]:
        """物流延迟长链推理"""
        chain = []
        order = self.ds.orders.get(signal.metrics.get("order_id", ""))
        
        chain.append(f"现象：订单 {signal.metrics.get('order_id')} 物流状态为延迟")
        
        if signal.external_factors:
            chain.append(f"直接原因：受外部事件影响 - {', '.join(signal.external_factors)}")
            chain.append("深层原因：极端天气/突发事件导致区域物流网络中断")
            chain.append("根因：缺乏多区域备货策略，单点物流依赖度过高")
        else:
            chain.append("直接原因：承运商配送效率下降")
            chain.append(f"深层原因：{order.carrier if order else '未知承运商'} 该线路近期准点率低于 80%")
            chain.append("根因：承运商选择未考虑实时履约能力，仅基于成本决策")
            
        return chain
        
    def _diagnose_demand_surge(self, signal: AnomalySignal) -> List[str]:
        """需求激增长链推理"""
        chain = []
        multiplier = signal.metrics.get("multiplier", 1.0)
        
        chain.append(f"现象：销量突增 {multiplier:.1f} 倍")
        
        # 检查是否有促销
        if multiplier > 3:
            chain.append("直接原因：疑似大促/爆款效应，非自然增长")
            chain.append("深层原因：营销活动未同步供应链备货计划")
            chain.append("根因：营供协同机制缺失，促销计划未触发安全库存调整")
        else:
            chain.append("直接原因：季节性/趋势性需求上升")
            chain.append("深层原因：需求预测模型未纳入实时社交媒体/搜索趋势数据")
            chain.append("根因：预测模型滞后，依赖历史销量而非实时信号")
            
        return chain
        
    def _diagnose_overstock(self, signal: AnomalySignal) -> List[str]:
        """库存积压长链推理"""
        chain = []
        days = signal.metrics.get("days_of_stock", 0)
        
        chain.append(f"现象：库存周转天数高达 {days:.0f} 天")
        chain.append("直接原因：销量低迷，出库速度远低于预期")
        chain.append("深层原因：前期采购过量或产品生命周期进入衰退期")
        chain.append("根因：缺乏动态库存健康度评估，采购决策与销售数据脱节")
        
        return chain
        
    def _diagnose_supplier_risk(self, signal: AnomalySignal) -> List[str]:
        """供应商风险长链推理"""
        chain = []
        sid = signal.metrics.get("supplier_id", "")
        supplier = self.ds.suppliers.get(sid)
        
        chain.append(f"现象：供应商 {supplier.name if supplier else sid} 合作中断")
        chain.append("直接原因：供应商主动终止合作或我方触发退出机制")
        chain.append("深层原因：供应商集中度风险，该供应商承担多个 SKU 供货")
        chain.append("根因：供应商评估体系偏重价格，忽视供应韧性（多源备份、BCP）")
        
        return chain
        
    def _gather_evidence(self, signal: AnomalySignal, chain: List[str]) -> List[str]:
        """收集支撑证据"""
        evidence = []
        product = self.ds.products.get(signal.product_id)
        
        if product:
            evidence.append(f"库存数据：{product.current_stock}/{product.safety_stock}（当前/安全）")
            evidence.append(f"销售基准：日均 {product.daily_sales_avg:.1f} 件")
            
        if signal.related_orders:
            evidence.append(f"关联订单：{len(signal.related_orders)} 个")
            
        if signal.external_factors:
            evidence.append(f"外部因素：{', '.join(signal.external_factors)}")
            
        return evidence

# ==================== Agent 3: 决策 Agent ====================

class DecisionAgent:
    """
    决策 Agent: 多方案评估与最优决策
    模拟多 Agent 协作评估不同处置方案
    """
    
    def __init__(self, data_simulator: DataSimulator):
        self.ds = data_simulator
        
    def generate_plans(self, signal: AnomalySignal, root_cause: RootCause) -> List[ResolutionPlan]:
        """基于根因生成多个候选方案"""
        plans = []
        product = self.ds.products.get(signal.product_id)
        
        if signal.anomaly_type == AnomalyType.STOCKOUT:
            plans = self._plans_for_stockout(signal, root_cause, product)
        elif signal.anomaly_type == AnomalyType.DELAY:
            plans = self._plans_for_delay(signal, root_cause, product)
        elif signal.anomaly_type == AnomalyType.DEMAND_SURGE:
            plans = self._plans_for_surge(signal, root_cause, product)
        elif signal.anomaly_type == AnomalyType.OVERSTOCK:
            plans = self._plans_for_overstock(signal, root_cause, product)
        elif signal.anomaly_type == AnomalyType.SUPPLIER_RISK:
            plans = self._plans_for_supplier_risk(signal, root_cause, product)
            
        # 按成功概率排序
        plans.sort(key=lambda p: p.success_probability, reverse=True)
        return plans
        
    def evaluate_and_select(self, plans: List[ResolutionPlan], signal: AnomalySignal) -> ResolutionPlan:
        """评估并选择最优方案"""
        if not plans:
            return None
            
        # 紧急情况下选择最快可执行的方案
        if signal.level == AnomalyLevel.EMERGENCY:
            executable = [p for p in plans if p.auto_executable]
            if executable:
                return max(executable, key=lambda p: p.success_probability)
            return max(plans, key=lambda p: p.success_probability)
            
        # 一般情况下选择性价比最优
        def score(p: ResolutionPlan) -> float:
            return (p.success_probability * 100) / (p.estimated_cost + 1)
            
        return max(plans, key=score)
        
    def _plans_for_stockout(self, signal, root_cause, product) -> List[ResolutionPlan]:
        plans = []
        
        # 方案1: 跨仓调拨
        for wid, wh in self.ds.warehouses.items():
            if wid != product.warehouse_id and wh.current_load < wh.capacity * 0.8:
                # 查找该仓库是否有库存
                # 简化：假设其他仓库可能有
                transfer_qty = min(500, wh.capacity - wh.current_load)
                cost = transfer_qty * 2 + 500  # 运费 + 操作费
                plans.append(ResolutionPlan(
                    plan_id=f"PLAN-{signal.id}-T",
                    action_type=ActionType.TRANSFER,
                    description=f"从 {wh.name} 紧急调拨 {transfer_qty} 件 {product.name}",
                    estimated_cost=cost,
                    estimated_time_hours=24,
                    success_probability=0.92,
                    affected_products=[product.id],
                    required_approvals=[],
                    auto_executable=signal.level == AnomalyLevel.EMERGENCY
                ))
                
        # 方案2: 紧急采购
        if product.supplier_id in self.ds.suppliers:
            supplier = self.ds.suppliers[product.supplier_id]
            qty = max(product.reorder_point * 3, 500)
            cost = qty * product.unit_cost * 1.2  # 加急溢价 20%
            plans.append(ResolutionPlan(
                plan_id=f"PLAN-{signal.id}-P",
                action_type=ActionType.URGENT_PURCHASE,
                description=f"向 {supplier.name} 紧急采购 {qty} 件，加急处理",
                estimated_cost=cost,
                estimated_time_hours=supplier.avg_lead_time * 24 * 0.5,  # 加急减半
                success_probability=supplier.reliability_score,
                affected_products=[product.id],
                required_approvals=["采购经理"] if cost > 50000 else [],
                auto_executable=cost < 50000 and signal.level == AnomalyLevel.EMERGENCY
            ))
            
        # 方案3: 切换预售
        plans.append(ResolutionPlan(
            plan_id=f"PLAN-{signal.id}-PRE",
            action_type=ActionType.PREORDER_SWITCH,
            description=f"将 {product.name} 切换为预售模式，承诺 15 天内发货",
            estimated_cost=0,
            estimated_time_hours=1,
            success_probability=0.75,
            affected_products=[product.id],
            required_approvals=["运营总监"],
            auto_executable=False
        ))
        
        return plans
        
    def _plans_for_delay(self, signal, root_cause, product) -> List[ResolutionPlan]:
        plans = []
        
        # 方案1: 更换承运商
        plans.append(ResolutionPlan(
            plan_id=f"PLAN-{signal.id}-C",
            action_type=ActionType.TRANSFER,
            description="拦截原订单，更换为顺丰特快重新发货",
            estimated_cost=signal.metrics.get("quantity", 50) * 15,
            estimated_time_hours=12,
            success_probability=0.88,
            affected_products=[product.id],
            required_approvals=[],
            auto_executable=True
        ))
        
        # 方案2: 安抚+补偿
        plans.append(ResolutionPlan(
            plan_id=f"PLAN-{signal.id}-S",
            action_type=ActionType.MANUAL_REVIEW,
            description="主动通知客户延迟原因，提供 20% 优惠券补偿",
            estimated_cost=500,
            estimated_time_hours=4,
            success_probability=0.85,
            affected_products=[product.id],
            required_approvals=["客服主管"],
            auto_executable=False
        ))
        
        return plans
        
    def _plans_for_surge(self, signal, root_cause, product) -> List[ResolutionPlan]:
        plans = []
        
        # 方案1: 多仓联动+紧急采购
        qty = int(signal.metrics.get("avg_7d_sales", 100) * 14)  # 备两周货
        plans.append(ResolutionPlan(
            plan_id=f"PLAN-{signal.id}-B",
            action_type=ActionType.URGENT_PURCHASE,
            description=f"紧急备货 {qty} 件，全仓分发，锁定供应商产能",
            estimated_cost=qty * product.unit_cost,
            estimated_time_hours=72,
            success_probability=0.80,
            affected_products=[product.id],
            required_approvals=["供应链总监"],
            auto_executable=False
        ))
        
        # 方案2: 预售控单
        plans.append(ResolutionPlan(
            plan_id=f"PLAN-{signal.id}-L",
            action_type=ActionType.PREORDER_SWITCH,
            description="开启限量预售，控制日销节奏，避免一次性断货",
            estimated_cost=0,
            estimated_time_hours=2,
            success_probability=0.90,
            affected_products=[product.id],
            required_approvals=["运营经理"],
            auto_executable=True
        ))
        
        return plans
        
    def _plans_for_overstock(self, signal, root_cause, product) -> List[ResolutionPlan]:
        plans = []
        
        # 方案1: 降价清仓
        plans.append(ResolutionPlan(
            plan_id=f"PLAN-{signal.id}-D",
            action_type=ActionType.DISCOUNT_CLEAR,
            description="启动 7 折限时闪购，3 天清仓目标",
            estimated_cost=product.current_stock * product.selling_price * 0.3,  # 毛利损失
            estimated_time_hours=72,
            success_probability=0.78,
            affected_products=[product.id],
            required_approvals=["营销总监"],
            auto_executable=False
        ))
        
        # 方案2: 跨区调拨至热销仓
        plans.append(ResolutionPlan(
            plan_id=f"PLAN-{signal.id}-M",
            action_type=ActionType.TRANSFER,
            description="调拨至销售速度更快的华东仓",
            estimated_cost=product.current_stock * 3,
            estimated_time_hours=48,
            success_probability=0.65,
            affected_products=[product.id],
            required_approvals=[],
            auto_executable=True
        ))
        
        return plans
        
    def _plans_for_supplier_risk(self, signal, root_cause, product) -> List[ResolutionPlan]:
        plans = []
        
        affected = signal.metrics.get("affected_products", [])
        
        # 方案1: 切换备用供应商
        backup_suppliers = [
            s for s in self.ds.suppliers.values() 
            if s.id != signal.metrics.get("supplier_id") and s.is_active
        ]
        for bs in backup_suppliers:
            plans.append(ResolutionPlan(
                plan_id=f"PLAN-{signal.id}-BS-{bs.id}",
                action_type=ActionType.SUPPLIER_SWITCH,
                description=f"切换至备用供应商 {bs.name}，重新谈判交期与价格",
                estimated_cost=len(affected) * 10000,  # 切换成本
                estimated_time_hours=bs.avg_lead_time * 24,
                success_probability=bs.reliability_score,
                affected_products=affected,
                required_approvals=["采购总监", "财务总监"],
                auto_executable=False
            ))
            
        # 方案2: 现货市场采购（高价）
        plans.append(ResolutionPlan(
            plan_id=f"PLAN-{signal.id}-SP",
            action_type=ActionType.URGENT_PURCHASE,
            description="现货市场高价采购，保障短期供应不断",
            estimated_cost=len(affected) * 50000,
            estimated_time_hours=48,
            success_probability=0.60,
            affected_products=affected,
            required_approvals=["CEO"],
            auto_executable=False
        ))
        
        return plans

# ==================== Agent 4: 执行 Agent ====================

class ExecutionAgent:
    """
    执行 Agent: 自动执行与闭环反馈
    - 自动执行无需审批的方案
    - 人工审批流管理
    - 执行结果追踪与反馈
    """
    
    def __init__(self, data_simulator: DataSimulator):
        self.ds = data_simulator
        self.execution_log: deque = deque(maxlen=500)
        self.pending_approvals: Dict[str, ResolutionPlan] = {}
        
    def execute(self, plan: ResolutionPlan, signal: AnomalySignal) -> ExecutionResult:
        """执行方案"""
        start_time = time.time()
        
        if plan.required_approvals and not plan.auto_executable:
            # 需要人工审批
            self.pending_approvals[plan.plan_id] = plan
            return ExecutionResult(
                plan_id=plan.plan_id,
                executed_at=datetime.now(),
                status="pending_approval",
                details={
                    "message": f"等待审批：{', '.join(plan.required_approvals)}",
                    "signal_id": signal.id
                },
                cost_incurred=0,
                time_taken_minutes=0,
                follow_up_required=True
            )
            
        # 自动执行
        details = self._perform_action(plan, signal)
        elapsed = (time.time() - start_time) / 60
        
        result = ExecutionResult(
            plan_id=plan.plan_id,
            executed_at=datetime.now(),
            status="success",
            details=details,
            cost_incurred=plan.estimated_cost * random.uniform(0.9, 1.1),  # 实际成本浮动
            time_taken_minutes=elapsed,
            follow_up_required=plan.action_type in [ActionType.URGENT_PURCHASE, ActionType.SUPPLIER_SWITCH]
        )
        
        self.execution_log.append(result)
        return result
        
    def _perform_action(self, plan: ResolutionPlan, signal: AnomalySignal) -> Dict:
        """模拟执行具体动作"""
        details = {"action": plan.action_type.value}
        product = self.ds.products.get(signal.product_id)
        
        if plan.action_type == ActionType.TRANSFER:
            # 模拟调拨：增加目标仓库存，减少源仓
            if product:
                transfer_qty = min(product.current_stock, 500)
                product.current_stock -= transfer_qty  # 简化：直接扣减
                details["transferred_qty"] = transfer_qty
                details["from_warehouse"] = product.warehouse_id
                
        elif plan.action_type == ActionType.URGENT_PURCHASE:
            # 模拟采购：创建在途订单
            order = Order(
                id=f"URG-{datetime.now().strftime('%Y%m%d%H%M%S')}",
                product_id=signal.product_id,
                quantity=int(plan.description.split(" ")[2]) if " " in plan.description else 100,
                status="pending",
                created_at=datetime.now(),
                estimated_delivery=datetime.now() + timedelta(hours=plan.estimated_time_hours)
            )
            self.ds.orders[order.id] = order
            details["purchase_order_id"] = order.id
            
        elif plan.action_type == ActionType.PREORDER_SWITCH:
            details["mode_switched"] = True
            details["estimated_fulfillment"] = "15天内"
            
        elif plan.action_type == ActionType.DISCOUNT_CLEAR:
            details["discount_rate"] = 0.7
            details["campaign_launched"] = True
            
        elif plan.action_type == ActionType.SUPPLIER_SWITCH:
            # 模拟切换供应商
            if product:
                old_supplier = product.supplier_id
                # 选择新供应商
                new_supplier = random.choice([
                    s.id for s in self.ds.suppliers.values() 
                    if s.id != old_supplier and s.is_active
                ])
                product.supplier_id = new_supplier
                details["old_supplier"] = old_supplier
                details["new_supplier"] = new_supplier
                
        return details
        
    def approve(self, plan_id: str, approver: str) -> Optional[ExecutionResult]:
        """人工审批通过"""
        plan = self.pending_approvals.pop(plan_id, None)
        if not plan:
            return None
            
        # 重新执行
        # 创建虚拟 signal 用于执行
        class DummySignal:
            pass
        ds = DummySignal()
        ds.id = plan.plan_id
        ds.product_id = plan.affected_products[0] if plan.affected_products else ""
        
        # 查找真实 signal
        # 简化处理
        return self.execute(plan, AnomalySignal(
            id=plan.plan_id, timestamp=datetime.now(),
            anomaly_type=AnomalyType.STOCKOUT, level=AnomalyLevel.WARNING,
            product_id=plan.affected_products[0] if plan.affected_products else "",
            warehouse_id="", description="", metrics={}
        ))
        
    def get_pending_approvals(self) -> List[Dict]:
        """获取待审批列表"""
        return [
            {
                "plan_id": pid,
                "description": p.description,
                "cost": p.estimated_cost,
                "required": p.required_approvals
            }
            for pid, p in self.pending_approvals.items()
        ]

# ==================== 主控系统 ====================

class SupplyChainAgentSystem:
    """
    供应链异常预警与自动处置 Agent 系统主控
    协调多 Agent 协作
    """
    
    def __init__(self):
        print("=" * 60)
        print("供应链异常预警与自动处置 Agent MVP")
        print("Supply Chain Anomaly Detection & Auto-Resolution Agent")
        print("=" * 60)
        
        self.ds = DataSimulator()
        self.monitor = MonitoringAgent(self.ds)
        self.diagnosis = DiagnosisAgent(self.ds)
        self.decision = DecisionAgent(self.ds)
        self.execution = ExecutionAgent(self.ds)
        
        self.resolved_count = 0
        self.auto_resolved_count = 0
        self.pending_count = 0
        
        # 注册监控回调
        self.monitor.register_callback(self._on_anomaly_detected)
        
    def _on_anomaly_detected(self, signal: AnomalySignal):
        """异常检测回调：触发诊断-决策-执行链"""
        print(f"\n{'='*60}")
        print(f"🚨 异常 detected: [{signal.level.value.upper()}] {signal.anomaly_type.value}")
        print(f"   商品: {self.ds.products.get(signal.product_id, Product('','','','','','',0,0,0,0,0,0,0)).name}")
        print(f"   描述: {signal.description}")
        print(f"   置信度: {signal.confidence:.0%}")
        
        # Step 1: 诊断 Agent - 长链推理
        print(f"\n🔍 [诊断 Agent] 启动长链推理...")
        root_cause = self.diagnosis.diagnose(signal)
        print(f"   根因: {root_cause.primary_cause}")
        print(f"   推理链:")
        for i, step in enumerate(root_cause.causal_chain, 1):
            print(f"      {i}. {step}")
        print(f"   证据:")
        for ev in root_cause.evidence:
            print(f"      • {ev}")
            
        # Step 2: 决策 Agent - 多方案评估
        print(f"\n🧠 [决策 Agent] 生成候选方案...")
        plans = self.decision.generate_plans(signal, root_cause)
        
        for i, plan in enumerate(plans, 1):
            auto_tag = "⚡自动" if plan.auto_executable else "👤人工"
            print(f"   方案{i}: [{plan.action_type.value}] {auto_tag}")
            print(f"      {plan.description}")
            print(f"      成本: ¥{plan.estimated_cost:,.0f} | 时间: {plan.estimated_time_hours:.0f}h | 成功率: {plan.success_probability:.0%}")
            
        best_plan = self.decision.evaluate_and_select(plans, signal)
        if best_plan:
            print(f"\n   ⭐ 最优方案: [{best_plan.action_type.value}] {best_plan.description}")
            
            # Step 3: 执行 Agent
            print(f"\n⚙️  [执行 Agent] 执行方案...")
            result = self.execution.execute(best_plan, signal)
            
            print(f"   状态: {result.status}")
            print(f"   耗时: {result.time_taken_minutes:.1f} 分钟")
            print(f"   实际成本: ¥{result.cost_incurred:,.0f}")
            
            if result.status == "pending_approval":
                self.pending_count += 1
                print(f"   ⏳ 已提交审批，等待: {', '.join(best_plan.required_approvals)}")
            else:
                self.resolved_count += 1
                if best_plan.auto_executable:
                    self.auto_resolved_count += 1
                print(f"   ✅ 执行完成，详情: {result.details}")
                
            if result.follow_up_required:
                print(f"   🔔 已设置后续跟踪任务")
        else:
            print("   ❌ 无可行方案，已转人工处理")
            
        print(f"{'='*60}")
        
    def start(self):
        """启动系统"""
        self.ds.start_simulation()
        time.sleep(1)  # 等待数据初始化
        self.monitor.start_monitoring()
        
    def stop(self):
        """停止系统"""
        self.ds.stop_simulation()
        self.monitor._running = False
        
    def get_stats(self) -> Dict:
        """获取运行统计"""
        return {
            "total_anomalies": len(self.monitor.anomaly_history),
            "resolved": self.resolved_count,
            "auto_resolved": self.auto_resolved_count,
            "pending_approval": self.pending_count,
            "pending_list": self.execution.get_pending_approvals(),
            "current_data": self.ds.get_current_state()
        }
        
    def print_dashboard(self):
        """打印实时仪表盘"""
        stats = self.get_stats()
        print(f"\n{'='*60}")
        print("📊 系统运行仪表盘")
        print(f"{'='*60}")
        print(f"总异常检测: {stats['total_anomalies']}")
        print(f"已自动处置: {stats['auto_resolved']} | 已处置(含人工): {stats['resolved']}")
        print(f"待审批: {stats['pending_approval']}")
        
        if stats['pending_list']:
            print(f"\n⏳ 待审批列表:")
            for p in stats['pending_list']:
                print(f"   • {p['plan_id']}: ¥{p['cost']:,.0f} - 需 {', '.join(p['required'])}")
                
        print(f"\n📦 实时库存快照:")
        for pid, p in self.ds.products.items():
            status = "🟢" if p.current_stock > p.safety_stock else "🔴"
            print(f"   {status} {p.name:20s} | 库存: {p.current_stock:4d} | 安全线: {p.safety_stock:3d}")
            
        print(f"{'='*60}")


# ==================== 运行入口 ====================

def main():
    """主程序入口"""
    system = SupplyChainAgentSystem()
    
    try:
        print("\n🚀 启动供应链 Agent 系统...")
        system.start()
        
        print("\n系统运行中，每 3 秒扫描一次异常...")
        print("按 Ctrl+C 停止并查看最终统计\n")
        
        # 运行 60 秒演示
        for i in range(20):
            time.sleep(3)
            system.print_dashboard()
            
    except KeyboardInterrupt:
        print("\n\n🛑 收到停止信号...")
    finally:
        system.stop()
        
        print(f"\n{'='*60}")
        print("📈 最终运行报告")
        print(f"{'='*60}")
        stats = system.get_stats()
        print(f"总异常检测: {stats['total_anomalies']}")
        print(f"自动处置: {stats['auto_resolved']} ({stats['auto_resolved']/max(stats['total_anomalies'],1)*100:.1f}%)")
        print(f"总处置完成: {stats['resolved']}")
        print(f"待审批: {stats['pending_approval']}")
        print(f"\n系统已停止")
        print(f"{'='*60}")


if __name__ == "__main__":
    main()
