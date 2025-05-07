<%--
  Created by IntelliJ IDEA.
  User: Z
  Date: 2017/10/28
  Time: 14:36
  To change this template use File | Settings | File Templates.
--%>
<%@ page contentType="text/html;charset=UTF-8" language="java" isELIgnored="false" %>
<html>
<head>
    <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
    <title>显示电商数据</title>
    <script type="text/javascript" src="../js/echarts.min.js"></script>
</head>
<body style="height: 100%; margin: 0; background-color: #3C3F41">
<style type="text/css">
    h3 {
        font-size: 12px;
        color: #ffffff;
        display: inline
    }
</style>
<h4 style="color: #ffffff;text-align:center">电商月单查询：${requestScope.user_name}</h4>
<div id="container1" style="height: 80%; width: 50%; float:left"></div>
<div id="container2" style="height: 80%; width: 50%; float:right"></div>
<script type="text/javascript">
    var user_id = "${requestScope.user_id}"
    var user_name = "${requestScope.user_name}"

    var date = "${requestScope.date}"//1月,2月,3月,xxxxx
    var order_count = "${requestScope.order_count}"
    var total_amount = "${requestScope.total_amount}"
    var pieData = converterFun(total_amount.split(","), date.split(","))
    purchaseStats1();
    purchaseStats2();

    function converterFun(total_amount, date) {
        var array = [];
        for (var i = 0; i < total_amount.length; i++) {
            var map = {};
            map['value'] = parseFloat(total_amount[i]);
            map['name'] = date[i];
            array.push(map);
        }
        return array;
    }

    function purchaseStats1() {
        var dom = document.getElementById("container1");
        var myChart = echarts.init(dom);
        myChart.showLoading();
        var option = {
            title: {
                text: '订单数量',
                textStyle: {
                    color: '#ffffff',
                    fontStyle: 'normal',
                    fontWeight: 'bold',
                    fontFamily: 'sans-serif',
                    fontSize: 13
                },
                itemGap: 12,
            },
            grid: {
                x: 80,
                y: 60,
                x2: 80,
                y2: 60,
                backgroundColor: 'rgba(0,0,0,0)',
                borderWidth: 1,
                borderColor: '#ffffff'
            },
            tooltip: {
                trigger: 'axis'
            },
            legend: {
                borderColor: '#ffffff',
                itemGap: 10,
                data: ['订单数量'],
                textStyle: {
                    color: '#ffffff'
                }
            },
            toolbox: {
                show: false,
                feature: {
                    dataZoom: {
                        yAxisIndex: 'none'
                    },
                    dataView: {readOnly: false},
                    magicType: {type: ['line', 'bar']},
                    restore: {},
                    saveAsImage: {}
                }
            },
            xAxis: {
                data: date.split(","),
                axisLine: {
                    lineStyle: {
                        color: '#ffffff',
                        width: 2
                    }
                }
            },
            yAxis: {
                axisLine: {
                    lineStyle: {
                        color: '#ffffff',
                        width: 2
                    }
                }
            },
            series: [
                {
                    type: 'line',
                    data: order_count.split(","),
                    itemStyle: {
                        normal: {
                            color: '#ffca29',
                            lineStyle: {
                                color: '#ffd80d',
                                width: 2
                            }
                        }
                    },
                    markPoint: {
                        data: [
                            {type: 'max', name: '最大值'},
                            {type: 'min', name: '最小值'}
                        ]
                    },
                    markLine: {
                        data: [
                            {type: 'average', name: '平均值'}
                        ]
                    }
                }
            ]
        };
        if (option && typeof option === "object") {
            myChart.setOption(option, true);
        }
        myChart.hideLoading()
    }

    function purchaseStats2() {
        var dom = document.getElementById("container2");
        var myChart = echarts.init(dom);
        myChart.showLoading();
        var option = {
            title: {
                text: '总金额',
                textStyle: {
                    color: '#ffffff',
                    fontStyle: 'normal',
                    fontWeight: 'bold',
                    fontFamily: 'sans-serif',
                    fontSize: 13
                },
                itemGap: 12,
            },
            tooltip: {
                trigger: 'item',
                formatter: "{a} <br/>{b} : {c} ({d}%)"
            },
            visualMap: {
                show: false,
                min: Math.min.apply(null, total_amount.split(",")),
                max: Math.max.apply(null, total_amount.split(",")),
                inRange: {
                    colorLightness: [0, 0.5]
                }
            },
            series: [
                {
                    name: '总金额',
                    type: 'pie',
                    radius: '55%',
                    center: ['50%', '50%'],
                    data: pieData.sort(function (a, b) {
                        return a.value - b.value;
                    }),
                    roseType: 'radius',
                    label: {
                        normal: {
                            textStyle: {
                                color: 'rgba(255, 255, 255, 0.3)'
                            }
                        }
                    },
                    labelLine: {
                        normal: {
                            lineStyle: {
                                color: 'rgba(255, 255, 255, 0.3)'
                            },
                            smooth: 0.2,
                            length: 10,
                            length2: 20
                        }
                    },
                    itemStyle: {
                        normal: {
                            color: '#01c1c2',
                            shadowBlur: 200,
                            shadowColor: 'rgba(0, 0, 0, 0.5)'
                        }
                    },

                    animationType: 'scale',
                    animationEasing: 'elasticOut',
                    animationDelay: function (idx) {
                        return Math.random() * 200;
                    }
                }
            ]
        };
        if (option && typeof option === "object") {
            myChart.setOption(option, true);
        }
        myChart.hideLoading()
    }
</script>
</body>
</html>