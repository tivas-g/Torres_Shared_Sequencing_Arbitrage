
import time

from math import sqrt

def swap_V2(amount_in, reserve_in, reserve_out):
    reserves = [
        reserve_in,
        reserve_out,
    ]

    # calculate constant for CMM function
    k = reserves[0] * reserves[1]

    # apply pool fee
    amount_in *= 1 - 0.003
    
    # calculate amount out
    amount_out = reserves[1] - (k / (reserves[0] + amount_in))
        
    return amount_out

def getAmountDelta(priceA, priceB, liquidity, zeroForOne):
    if zeroForOne:
        if priceA > priceB:
            priceA, priceB = priceB, priceA
        return int(liquidity * ((1 / priceA) - (1 / priceB)))
    else:
        if priceA < priceB:
            priceA, priceB = priceB, priceA
        return int(liquidity * (priceA - priceB))

def calc_sqrt_price(i):
    sqrtPrice = 1.0001 ** (i / 2.0)
    if sqrtPrice > 1_000_000_000_000:
        return int(sqrtPrice)
    return sqrtPrice

def getNextSqrtPriceFromAmount(price, liquidity, amount, zeroForOne):
    if zeroForOne:
        return liquidity * price / (liquidity + amount * price)
    else:
        return price + (amount / liquidity)

def swap_V3(amount_in, zeroForOne, current_tick, current_price, liquidity, fee_tier, ticks):
    current_price = current_price / (2**96)

    tick_spacing = None
    if   fee_tier == "100":
        tick_spacing = 1
    elif fee_tier == "500":
        tick_spacing = 10
    elif fee_tier == "3000":
        tick_spacing = 60
    elif fee_tier == "10000":
        tick_spacing = 200
        
    # apply pool fee
    fee_perc = float(fee_tier) / 1_000_000
    amount_in = float(amount_in) * (1.0 - fee_perc)

    # set amounts
    amount_remaining = amount_in
    amount_out = 0

    # swap until no input is left or loop limit is reached
    loop_counter = 0
    loop_limit = 100
    while amount_remaining > 0:# and loop_counter <= loop_limit:
        loop_counter += 1

        # get the next tick
        if current_tick % tick_spacing != 0:
            if zeroForOne:
                next_tick = current_tick - (current_tick % tick_spacing)
            else:
                next_tick = current_tick + tick_spacing - (current_tick % tick_spacing)
        else:
            if zeroForOne:
                next_tick = current_tick - tick_spacing
            else:
                next_tick = current_tick + tick_spacing
    
        next_price = calc_sqrt_price(int(next_tick))
        
        amount_to_next_tick = getAmountDelta(next_price, current_price, liquidity, zeroForOne)
        
        if amount_to_next_tick == 0:
            return amount_out

        if amount_to_next_tick > amount_remaining:
            final_price = getNextSqrtPriceFromAmount(current_price, liquidity, amount_remaining, zeroForOne)
            amount_out += getAmountDelta(final_price, current_price, liquidity, not zeroForOne)
            break
        else:
            temp_out = getAmountDelta(next_price, current_price, liquidity, not zeroForOne)
            amount_out += temp_out
            amount_remaining -= int(amount_to_next_tick)
            current_tick = next_tick
            current_price = next_price

        # update liquidity if next tick is initiallized
        try:
            if zeroForOne:
                liquidity += int(ticks[int(next_tick)])
            else:
                liquidity -= int(ticks[int(next_tick)])
        except KeyError as e:
            pass

    return amount_out
    
def router(route, amount_in, debug=False):
    amount_out = 0
    between_lp_amts = [amount_in]    
    for swap in route:
        if   swap["protocol"] == "uniswap" and swap["version"] == "v2":
            amount_out = swap_V2(
                amount_in, 
                int(swap["reserve_in"]),
                int(swap["reserve_out"])
            )
            if debug:
                print("Executed Uniswap V2 swap:", amount_in, "->", amount_out)
        elif swap["protocol"] == "uniswap" and swap["version"] == "v3":
            amount_out = swap_V3(
                amount_in,
                swap["zero_for_one"],
                int(swap["current_tick"]),
                int(swap["current_price"]),
                int(swap["liquidity"]),
                swap["fee_tier"],
                swap["ticks"]
            )
            if debug:
                print("Executed Uniswap V3 swap:", amount_in, "->", amount_out)
        else:
            print("Protocol version not found: Procotol: "+str(swap["protocol"]+" Version: "+str(swap["version"])))
            return 0
        
        amount_in = amount_out
        between_lp_amts.append(amount_in)
    if debug:
        print()

    return amount_out, between_lp_amts

def ternary_search(path, starting_r=None, debug=False):
    if starting_r == None:
        starting_r = int(0.1 * (10 ** int(path[0]["token_in_decimals"])))
    
    outer_done = False
    while not outer_done:
        l = 0
        r = starting_r

        done = False

        while not done:
            input0 = int((r - l) * (1 / 3) + l)
            input1 = int((r - l) * (2 / 3) + l)

            output0, _ = router(path, input0, debug)
            output1, between_lp_amts1 = router(path, input1, debug)

            profit0 = output0 - input0
            profit1 = output1 - input1

            if profit0 < profit1:
                l = input0
            elif profit0 > profit1:
                r = input1
            else:
                l = input0
                r = input1

            if abs(r - l) < starting_r * 0.1:
                done = True

        if r > starting_r * 0.9:
            starting_r *= 10
        else:
            outer_done = True

    return input1, profit1, between_lp_amts1
    
def cp_amm_out(R_in, R_out, d_in):
    return R_out - ((R_in * R_out) / (R_in + (0.997 * d_in)))

def fast_path_two_arb(R_in_0, R_out_0, R_in_1, R_out_1):
    solution1 = 1.00300902708124*(997000000.0*sqrt(R_in_0)*sqrt(R_in_1)*sqrt(R_out_0)*sqrt(R_out_1)*(R_in_1 + 0.997*R_out_0)**2 - 1000.0*R_in_0*R_in_1*(1000000.0*R_in_1**2 + 1994000.0*R_in_1*R_out_0 + 994009.0*R_out_0**2))/((1000.0*R_in_1 + 997.0*R_out_0)*(1000000.0*R_in_1**2 + 1994000.0*R_in_1*R_out_0 + 994009.0*R_out_0**2))
    solution2 = -0.00100300902708124*(997000000000.0*sqrt(R_in_0)*sqrt(R_in_1)*sqrt(R_out_0)*sqrt(R_out_1)*(R_in_1 + 0.997*R_out_0)**2 + 1000000.0*R_in_0*R_in_1*(1000000.0*R_in_1**2 + 1994000.0*R_in_1*R_out_0 + 994009.0*R_out_0**2))/((1000.0*R_in_1 + 997.0*R_out_0)*(1000000.0*R_in_1**2 + 1994000.0*R_in_1*R_out_0 + 994009.0*R_out_0**2))

    d_in_0 = max(solution1, solution2)

    if d_in_0 > 0:
        between_lp_amts = [d_in_0]
        between_lp_amts.append(cp_amm_out(R_in_0, R_out_0, between_lp_amts[-1]))
        between_lp_amts.append(cp_amm_out(R_in_1, R_out_1, between_lp_amts[-1]))

        profit = max(between_lp_amts[-1] - d_in_0, 0)

        return  d_in_0, profit, between_lp_amts
    else:
        return 0, 0, None

def fast_path_three_arb(R_in_0, R_out_0, R_in_1, R_out_1, R_in_2, R_out_2):
    solution1 = 10.0300902708124*(9.95503376689401e+16*sqrt(R_in_0)*sqrt(R_in_1)*sqrt(R_in_2)*sqrt(R_out_0)*sqrt(R_out_1)*sqrt(R_out_2)*(R_in_1*R_in_2 + 0.997*R_in_2*R_out_0 + 0.994009*R_out_0*R_out_1)**2 - 100000.0*R_in_0*R_in_1*R_in_2*(1000000000000.0*R_in_1**2*R_in_2**2 + 1994000000000.0*R_in_1*R_in_2**2*R_out_0 + 1988018000000.0*R_in_1*R_in_2*R_out_0*R_out_1 + 994009000000.0*R_in_2**2*R_out_0**2 + 1982053946000.0*R_in_2*R_out_0**2*R_out_1 + 988053892081.0*R_out_0**2*R_out_1**2))/((1000000.0*R_in_1*R_in_2 + 997000.0*R_in_2*R_out_0 + 994009.0*R_out_0*R_out_1)*(1000000000000.0*R_in_1**2*R_in_2**2 + 1994000000000.0*R_in_1*R_in_2**2*R_out_0 + 1988018000000.0*R_in_1*R_in_2*R_out_0*R_out_1 + 994009000000.0*R_in_2**2*R_out_0**2 + 1982053946000.0*R_in_2*R_out_0**2*R_out_1 + 988053892081.0*R_out_0**2*R_out_1**2))

    solution2 = -0.00100300902708124*(9.95503376689401e+20*sqrt(R_in_0)*sqrt(R_in_1)*sqrt(R_in_2)*sqrt(R_out_0)*sqrt(R_out_1)*sqrt(R_out_2)*(R_in_1*R_in_2 + 0.997*R_in_2*R_out_0 + 0.994009*R_out_0*R_out_1)**2 + 1000000000.0*R_in_0*R_in_1*R_in_2*(1000000000000.0*R_in_1**2*R_in_2**2 + 1994000000000.0*R_in_1*R_in_2**2*R_out_0 + 1988018000000.0*R_in_1*R_in_2*R_out_0*R_out_1 + 994009000000.0*R_in_2**2*R_out_0**2 + 1982053946000.0*R_in_2*R_out_0**2*R_out_1 + 988053892081.0*R_out_0**2*R_out_1**2))/((1000000.0*R_in_1*R_in_2 + 997000.0*R_in_2*R_out_0 + 994009.0*R_out_0*R_out_1)*(1000000000000.0*R_in_1**2*R_in_2**2 + 1994000000000.0*R_in_1*R_in_2**2*R_out_0 + 1988018000000.0*R_in_1*R_in_2*R_out_0*R_out_1 + 994009000000.0*R_in_2**2*R_out_0**2 + 1982053946000.0*R_in_2*R_out_0**2*R_out_1 + 988053892081.0*R_out_0**2*R_out_1**2))

    d_in_0 = max(solution1, solution2)

    if d_in_0 > 0:
        between_lp_amts = [d_in_0]
        between_lp_amts.append(cp_amm_out(R_in_0, R_out_0, between_lp_amts[-1]))
        between_lp_amts.append(cp_amm_out(R_in_1, R_out_1, between_lp_amts[-1]))
        between_lp_amts.append(cp_amm_out(R_in_2, R_out_2, between_lp_amts[-1]))

        profit = max(between_lp_amts[-1] - d_in_0, 0)

        return  d_in_0, profit, between_lp_amts
    else:
        return 0, 0, None

def fast_path_four_arb(R_in_0, R_out_0, R_in_1, R_out_1, R_in_2, R_out_2, R_in_3, R_out_3):
    solution1 = 1003.00902708124*(9.94009e+23*sqrt(R_in_0)*sqrt(R_in_1)*sqrt(R_in_2)*sqrt(R_in_3)*sqrt(R_out_0)*sqrt(R_out_1)*sqrt(R_out_2)*sqrt(R_out_3)*(R_in_1*R_in_2*R_in_3 + 0.997*R_in_2*R_in_3*R_out_0 + 0.994009*R_in_3*R_out_0*R_out_1 + 0.991026973*R_out_0*R_out_1*R_out_2)**2 - 1000000.0*R_in_0*R_in_1*R_in_2*R_in_3*(1.0e+18*R_in_1**2*R_in_2**2*R_in_3**2 + 1.994e+18*R_in_1*R_in_2**2*R_in_3**2*R_out_0 + 1.988018e+18*R_in_1*R_in_2*R_in_3**2*R_out_0*R_out_1 + 1.982053946e+18*R_in_1*R_in_2*R_in_3*R_out_0*R_out_1*R_out_2 + 9.94009e+17*R_in_2**2*R_in_3**2*R_out_0**2 + 1.982053946e+18*R_in_2*R_in_3**2*R_out_0**2*R_out_1 + 1.976107784162e+18*R_in_2*R_in_3*R_out_0**2*R_out_1*R_out_2 + 9.88053892081e+17*R_in_3**2*R_out_0**2*R_out_1**2 + 1.97017946080951e+18*R_in_3*R_out_0**2*R_out_1**2*R_out_2 + 9.82134461213543e+17*R_out_0**2*R_out_1**2*R_out_2**2))/((1000000000.0*R_in_1*R_in_2*R_in_3 + 997000000.0*R_in_2*R_in_3*R_out_0 + 994009000.0*R_in_3*R_out_0*R_out_1 + 991026973.0*R_out_0*R_out_1*R_out_2)*(1.0e+18*R_in_1**2*R_in_2**2*R_in_3**2 + 1.994e+18*R_in_1*R_in_2**2*R_in_3**2*R_out_0 + 1.988018e+18*R_in_1*R_in_2*R_in_3**2*R_out_0*R_out_1 + 1.982053946e+18*R_in_1*R_in_2*R_in_3*R_out_0*R_out_1*R_out_2 + 9.94009e+17*R_in_2**2*R_in_3**2*R_out_0**2 + 1.982053946e+18*R_in_2*R_in_3**2*R_out_0**2*R_out_1 + 1.976107784162e+18*R_in_2*R_in_3*R_out_0**2*R_out_1*R_out_2 + 9.88053892081e+17*R_in_3**2*R_out_0**2*R_out_1**2 + 1.97017946080951e+18*R_in_3*R_out_0**2*R_out_1**2*R_out_2 + 9.82134461213543e+17*R_out_0**2*R_out_1**2*R_out_2**2))

    solution2 = -0.00100300902708124*(9.94009e+29*sqrt(R_in_0)*sqrt(R_in_1)*sqrt(R_in_2)*sqrt(R_in_3)*sqrt(R_out_0)*sqrt(R_out_1)*sqrt(R_out_2)*sqrt(R_out_3)*(R_in_1*R_in_2*R_in_3 + 0.997*R_in_2*R_in_3*R_out_0 + 0.994009*R_in_3*R_out_0*R_out_1 + 0.991026973*R_out_0*R_out_1*R_out_2)**2 + 1000000000000.0*R_in_0*R_in_1*R_in_2*R_in_3*(1.0e+18*R_in_1**2*R_in_2**2*R_in_3**2 + 1.994e+18*R_in_1*R_in_2**2*R_in_3**2*R_out_0 + 1.988018e+18*R_in_1*R_in_2*R_in_3**2*R_out_0*R_out_1 + 1.982053946e+18*R_in_1*R_in_2*R_in_3*R_out_0*R_out_1*R_out_2 + 9.94009e+17*R_in_2**2*R_in_3**2*R_out_0**2 + 1.982053946e+18*R_in_2*R_in_3**2*R_out_0**2*R_out_1 + 1.976107784162e+18*R_in_2*R_in_3*R_out_0**2*R_out_1*R_out_2 + 9.88053892081e+17*R_in_3**2*R_out_0**2*R_out_1**2 + 1.97017946080951e+18*R_in_3*R_out_0**2*R_out_1**2*R_out_2 + 9.82134461213543e+17*R_out_0**2*R_out_1**2*R_out_2**2))/((1000000000.0*R_in_1*R_in_2*R_in_3 + 997000000.0*R_in_2*R_in_3*R_out_0 + 994009000.0*R_in_3*R_out_0*R_out_1 + 991026973.0*R_out_0*R_out_1*R_out_2)*(1.0e+18*R_in_1**2*R_in_2**2*R_in_3**2 + 1.994e+18*R_in_1*R_in_2**2*R_in_3**2*R_out_0 + 1.988018e+18*R_in_1*R_in_2*R_in_3**2*R_out_0*R_out_1 + 1.982053946e+18*R_in_1*R_in_2*R_in_3*R_out_0*R_out_1*R_out_2 + 9.94009e+17*R_in_2**2*R_in_3**2*R_out_0**2 + 1.982053946e+18*R_in_2*R_in_3**2*R_out_0**2*R_out_1 + 1.976107784162e+18*R_in_2*R_in_3*R_out_0**2*R_out_1*R_out_2 + 9.88053892081e+17*R_in_3**2*R_out_0**2*R_out_1**2 + 1.97017946080951e+18*R_in_3*R_out_0**2*R_out_1**2*R_out_2 + 9.82134461213543e+17*R_out_0**2*R_out_1**2*R_out_2**2))

    d_in_0 = max(solution1, solution2)

    if d_in_0 > 0:
        between_lp_amts = [d_in_0]
        between_lp_amts.append(cp_amm_out(R_in_0, R_out_0, between_lp_amts[-1]))
        between_lp_amts.append(cp_amm_out(R_in_1, R_out_1, between_lp_amts[-1]))
        between_lp_amts.append(cp_amm_out(R_in_2, R_out_2, between_lp_amts[-1]))
        between_lp_amts.append(cp_amm_out(R_in_3, R_out_3, between_lp_amts[-1]))

        profit = max(between_lp_amts[-1] - d_in_0, 0)

        return  d_in_0, profit, between_lp_amts
    else:
        return 0, 0, None

def fast_path_five_arb(R_in_0, R_out_0, R_in_1, R_out_1, R_in_2, R_out_2, R_in_3, R_out_3, R_in_4, R_out_4):
    solution1 = 10030.0902708124*(9.92516866559333e+31*sqrt(R_in_0)*sqrt(R_in_1)*sqrt(R_in_2)*sqrt(R_in_3)*sqrt(R_in_4)*sqrt(R_out_0)*sqrt(R_out_1)*sqrt(R_out_2)*sqrt(R_out_3)*sqrt(R_out_4)*(R_in_1*R_in_2*R_in_3*R_in_4 + 0.997*R_in_2*R_in_3*R_in_4*R_out_0 + 0.994009*R_in_3*R_in_4*R_out_0*R_out_1 + 0.991026973*R_in_4*R_out_0*R_out_1*R_out_2 + 0.988053892081*R_out_0*R_out_1*R_out_2*R_out_3)**2 - 100000000.0*R_in_0*R_in_1*R_in_2*R_in_3*R_in_4*(1.0e+24*R_in_1**2*R_in_2**2*R_in_3**2*R_in_4**2 + 1.994e+24*R_in_1*R_in_2**2*R_in_3**2*R_in_4**2*R_out_0 + 1.988018e+24*R_in_1*R_in_2*R_in_3**2*R_in_4**2*R_out_0*R_out_1 + 1.982053946e+24*R_in_1*R_in_2*R_in_3*R_in_4**2*R_out_0*R_out_1*R_out_2 + 1.976107784162e+24*R_in_1*R_in_2*R_in_3*R_in_4*R_out_0*R_out_1*R_out_2*R_out_3 + 9.94009e+23*R_in_2**2*R_in_3**2*R_in_4**2*R_out_0**2 + 1.982053946e+24*R_in_2*R_in_3**2*R_in_4**2*R_out_0**2*R_out_1 + 1.976107784162e+24*R_in_2*R_in_3*R_in_4**2*R_out_0**2*R_out_1*R_out_2 + 1.97017946080951e+24*R_in_2*R_in_3*R_in_4*R_out_0**2*R_out_1*R_out_2*R_out_3 + 9.88053892081e+23*R_in_3**2*R_in_4**2*R_out_0**2*R_out_1**2 + 1.97017946080951e+24*R_in_3*R_in_4**2*R_out_0**2*R_out_1**2*R_out_2 + 1.96426892242709e+24*R_in_3*R_in_4*R_out_0**2*R_out_1**2*R_out_2*R_out_3 + 9.82134461213543e+23*R_in_4**2*R_out_0**2*R_out_1**2*R_out_2**2 + 1.9583761156598e+24*R_in_4*R_out_0**2*R_out_1**2*R_out_2**2*R_out_3 + 9.76250493656412e+23*R_out_0**2*R_out_1**2*R_out_2**2*R_out_3**2))/((1000000000000.0*R_in_1*R_in_2*R_in_3*R_in_4 + 997000000000.0*R_in_2*R_in_3*R_in_4*R_out_0 + 994009000000.0*R_in_3*R_in_4*R_out_0*R_out_1 + 991026973000.0*R_in_4*R_out_0*R_out_1*R_out_2 + 988053892081.0*R_out_0*R_out_1*R_out_2*R_out_3)*(1.0e+24*R_in_1**2*R_in_2**2*R_in_3**2*R_in_4**2 + 1.994e+24*R_in_1*R_in_2**2*R_in_3**2*R_in_4**2*R_out_0 + 1.988018e+24*R_in_1*R_in_2*R_in_3**2*R_in_4**2*R_out_0*R_out_1 + 1.982053946e+24*R_in_1*R_in_2*R_in_3*R_in_4**2*R_out_0*R_out_1*R_out_2 + 1.976107784162e+24*R_in_1*R_in_2*R_in_3*R_in_4*R_out_0*R_out_1*R_out_2*R_out_3 + 9.94009e+23*R_in_2**2*R_in_3**2*R_in_4**2*R_out_0**2 + 1.982053946e+24*R_in_2*R_in_3**2*R_in_4**2*R_out_0**2*R_out_1 + 1.976107784162e+24*R_in_2*R_in_3*R_in_4**2*R_out_0**2*R_out_1*R_out_2 + 1.97017946080951e+24*R_in_2*R_in_3*R_in_4*R_out_0**2*R_out_1*R_out_2*R_out_3 + 9.88053892081e+23*R_in_3**2*R_in_4**2*R_out_0**2*R_out_1**2 + 1.97017946080951e+24*R_in_3*R_in_4**2*R_out_0**2*R_out_1**2*R_out_2 + 1.96426892242709e+24*R_in_3*R_in_4*R_out_0**2*R_out_1**2*R_out_2*R_out_3 + 9.82134461213543e+23*R_in_4**2*R_out_0**2*R_out_1**2*R_out_2**2 + 1.9583761156598e+24*R_in_4*R_out_0**2*R_out_1**2*R_out_2**2*R_out_3 + 9.76250493656412e+23*R_out_0**2*R_out_1**2*R_out_2**2*R_out_3**2))

    solution2 = -0.00100300902708124*(9.92516866559333e+38*sqrt(R_in_0)*sqrt(R_in_1)*sqrt(R_in_2)*sqrt(R_in_3)*sqrt(R_in_4)*sqrt(R_out_0)*sqrt(R_out_1)*sqrt(R_out_2)*sqrt(R_out_3)*sqrt(R_out_4)*(R_in_1*R_in_2*R_in_3*R_in_4 + 0.997*R_in_2*R_in_3*R_in_4*R_out_0 + 0.994009*R_in_3*R_in_4*R_out_0*R_out_1 + 0.991026973*R_in_4*R_out_0*R_out_1*R_out_2 + 0.988053892081*R_out_0*R_out_1*R_out_2*R_out_3)**2 + 1.0e+15*R_in_0*R_in_1*R_in_2*R_in_3*R_in_4*(1.0e+24*R_in_1**2*R_in_2**2*R_in_3**2*R_in_4**2 + 1.994e+24*R_in_1*R_in_2**2*R_in_3**2*R_in_4**2*R_out_0 + 1.988018e+24*R_in_1*R_in_2*R_in_3**2*R_in_4**2*R_out_0*R_out_1 + 1.982053946e+24*R_in_1*R_in_2*R_in_3*R_in_4**2*R_out_0*R_out_1*R_out_2 + 1.976107784162e+24*R_in_1*R_in_2*R_in_3*R_in_4*R_out_0*R_out_1*R_out_2*R_out_3 + 9.94009e+23*R_in_2**2*R_in_3**2*R_in_4**2*R_out_0**2 + 1.982053946e+24*R_in_2*R_in_3**2*R_in_4**2*R_out_0**2*R_out_1 + 1.976107784162e+24*R_in_2*R_in_3*R_in_4**2*R_out_0**2*R_out_1*R_out_2 + 1.97017946080951e+24*R_in_2*R_in_3*R_in_4*R_out_0**2*R_out_1*R_out_2*R_out_3 + 9.88053892081e+23*R_in_3**2*R_in_4**2*R_out_0**2*R_out_1**2 + 1.97017946080951e+24*R_in_3*R_in_4**2*R_out_0**2*R_out_1**2*R_out_2 + 1.96426892242709e+24*R_in_3*R_in_4*R_out_0**2*R_out_1**2*R_out_2*R_out_3 + 9.82134461213543e+23*R_in_4**2*R_out_0**2*R_out_1**2*R_out_2**2 + 1.9583761156598e+24*R_in_4*R_out_0**2*R_out_1**2*R_out_2**2*R_out_3 + 9.76250493656412e+23*R_out_0**2*R_out_1**2*R_out_2**2*R_out_3**2))/((1000000000000.0*R_in_1*R_in_2*R_in_3*R_in_4 + 997000000000.0*R_in_2*R_in_3*R_in_4*R_out_0 + 994009000000.0*R_in_3*R_in_4*R_out_0*R_out_1 + 991026973000.0*R_in_4*R_out_0*R_out_1*R_out_2 + 988053892081.0*R_out_0*R_out_1*R_out_2*R_out_3)*(1.0e+24*R_in_1**2*R_in_2**2*R_in_3**2*R_in_4**2 + 1.994e+24*R_in_1*R_in_2**2*R_in_3**2*R_in_4**2*R_out_0 + 1.988018e+24*R_in_1*R_in_2*R_in_3**2*R_in_4**2*R_out_0*R_out_1 + 1.982053946e+24*R_in_1*R_in_2*R_in_3*R_in_4**2*R_out_0*R_out_1*R_out_2 + 1.976107784162e+24*R_in_1*R_in_2*R_in_3*R_in_4*R_out_0*R_out_1*R_out_2*R_out_3 + 9.94009e+23*R_in_2**2*R_in_3**2*R_in_4**2*R_out_0**2 + 1.982053946e+24*R_in_2*R_in_3**2*R_in_4**2*R_out_0**2*R_out_1 + 1.976107784162e+24*R_in_2*R_in_3*R_in_4**2*R_out_0**2*R_out_1*R_out_2 + 1.97017946080951e+24*R_in_2*R_in_3*R_in_4*R_out_0**2*R_out_1*R_out_2*R_out_3 + 9.88053892081e+23*R_in_3**2*R_in_4**2*R_out_0**2*R_out_1**2 + 1.97017946080951e+24*R_in_3*R_in_4**2*R_out_0**2*R_out_1**2*R_out_2 + 1.96426892242709e+24*R_in_3*R_in_4*R_out_0**2*R_out_1**2*R_out_2*R_out_3 + 9.82134461213543e+23*R_in_4**2*R_out_0**2*R_out_1**2*R_out_2**2 + 1.9583761156598e+24*R_in_4*R_out_0**2*R_out_1**2*R_out_2**2*R_out_3 + 9.76250493656412e+23*R_out_0**2*R_out_1**2*R_out_2**2*R_out_3**2))

    d_in_0 = max(solution1, solution2)

    if d_in_0 > 0:
        between_lp_amts = [d_in_0]
        between_lp_amts.append(cp_amm_out(R_in_0, R_out_0, between_lp_amts[-1]))
        between_lp_amts.append(cp_amm_out(R_in_1, R_out_1, between_lp_amts[-1]))
        between_lp_amts.append(cp_amm_out(R_in_2, R_out_2, between_lp_amts[-1]))
        between_lp_amts.append(cp_amm_out(R_in_3, R_out_3, between_lp_amts[-1]))
        between_lp_amts.append(cp_amm_out(R_in_4, R_out_4, between_lp_amts[-1]))

        profit = max(between_lp_amts[-1] - d_in_0, 0)

        return  d_in_0, profit, between_lp_amts
    else:
        return 0, 0, None

if __name__ == "__main__":
    print()
    print("Optimized Uniswap V2 fast arbitrage")
    print()
    
    start = time.time()
    result = fast_path_two_arb(100, 1000, 1000, 1000)
    print("Execution time:", (time.time() - start) * 1000)
    print("Input amount:", result[0])
    print("Output amount:", result[2][-1])
    print("Profit:", result[1])
    print("Swaps:", " -> ".join([str(x) for x in result[2]]))
    print()

    start = time.time()
    result = fast_path_three_arb(100, 1000, 1000, 1000, 1000, 1000)
    print("Execution time:", (time.time() - start) * 1000)
    print("Input amount:", result[0])
    print("Output amount:", result[2][-1])
    print("Profit:", result[1])
    print("Swaps:", " -> ".join([str(x) for x in result[2]]))
    print()

    start = time.time()
    result = fast_path_four_arb(100, 1000, 1000, 1000, 1000, 1000, 1000, 1000)
    print("Execution time:", (time.time() - start) * 1000)
    print("Input amount:", result[0])
    print("Output amount:", result[2][-1])
    print("Profit:", result[1])
    print("Swaps:", " -> ".join([str(x) for x in result[2]]))
    print()

    start = time.time()
    result = fast_path_five_arb(100, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000)
    print("Execution time:", (time.time() - start) * 1000)
    print("Input amount:", result[0])
    print("Output amount:", result[2][-1])
    print("Profit:", result[1])
    print("Swaps:", " -> ".join([str(x) for x in result[2]]))
    print()

    print()
    print()
    print("Ternary search Uniswap V2 arbitrage")
    print()
    
    start = time.time()
    path = [
        {
            'reserve_in': '100',
            'reserve_out': '1000',
            'token_in_decimals': '0',
            'token_out_decimals': '0',
            'variation': 'uniswap_v2'
        },
        {
            'reserve_in': '1000',
            'reserve_out': '1000',
            'token_in_decimals': '0',
            'token_out_decimals': '0',
            'variation': 'uniswap_v2'
        }
    ]
    result = ternary_search(path, 100)
    print("Execution time:", (time.time() - start) * 1000)
    print("Input amount:", result[0])
    print("Output amount:", result[2][-1])
    print("Profit:", result[1])
    print("Swaps:", " -> ".join([str(x) for x in result[2]]))
    print()

    start = time.time()
    path = [
        {
            'reserve_in': '100',
            'reserve_out': '1000',
            'token_in_decimals': '0',
            'token_out_decimals': '0',
            'variation': 'uniswap_v2'
        },
        {
            'reserve_in': '1000',
            'reserve_out': '1000',
            'token_in_decimals': '0',
            'token_out_decimals': '0',
            'variation': 'uniswap_v2'
        },
        {
            'reserve_in': '1000',
            'reserve_out': '1000',
            'token_in_decimals': '0',
            'token_out_decimals': '0',
            'variation': 'uniswap_v2'
        }
    ]
    result = ternary_search(path, 100)
    print("Execution time:", (time.time() - start) * 1000)
    print("Input amount:", result[0])
    print("Output amount:", result[2][-1])
    print("Profit:", result[1])
    print("Swaps:", " -> ".join([str(x) for x in result[2]]))
    print()

    start = time.time()
    path = [
        {
            'reserve_in': '100',
            'reserve_out': '1000',
            'token_in_decimals': '0',
            'token_out_decimals': '0',
            'variation': 'uniswap_v2'
        },
        {
            'reserve_in': '1000',
            'reserve_out': '1000',
            'token_in_decimals': '0',
            'token_out_decimals': '0',
            'variation': 'uniswap_v2'
        },
        {
            'reserve_in': '1000',
            'reserve_out': '1000',
            'token_in_decimals': '0',
            'token_out_decimals': '0',
            'variation': 'uniswap_v2'
        },
        {
            'reserve_in': '1000',
            'reserve_out': '1000',
            'token_in_decimals': '0',
            'token_out_decimals': '0',
            'variation': 'uniswap_v2'
        }
    ]
    result = ternary_search(path, 100)
    print("Execution time:", (time.time() - start) * 1000)
    print("Input amount:", result[0])
    print("Output amount:", result[2][-1])
    print("Profit:", result[1])
    print("Swaps:", " -> ".join([str(x) for x in result[2]]))
    print()

    start = time.time()
    path = [
        {
            'reserve_in': '100',
            'reserve_out': '1000',
            'token_in_decimals': '0',
            'token_out_decimals': '0',
            'variation': 'uniswap_v2'
        },
        {
            'reserve_in': '1000',
            'reserve_out': '1000',
            'token_in_decimals': '0',
            'token_out_decimals': '0',
            'variation': 'uniswap_v2'
        },
        {
            'reserve_in': '1000',
            'reserve_out': '1000',
            'token_in_decimals': '0',
            'token_out_decimals': '0',
            'variation': 'uniswap_v2'
        },
        {
            'reserve_in': '1000',
            'reserve_out': '1000',
            'token_in_decimals': '0',
            'token_out_decimals': '0',
            'variation': 'uniswap_v2'
        },
        {
            'reserve_in': '1000',
            'reserve_out': '1000',
            'token_in_decimals': '0',
            'token_out_decimals': '0',
            'variation': 'uniswap_v2'
        }
    ]
    result = ternary_search(path, 100)
    print("Execution time:", (time.time() - start) * 1000)
    print("Input amount:", result[0])
    print("Output amount:", result[2][-1])
    print("Profit:", result[1])
    print("Swaps:", " -> ".join([str(x) for x in result[2]]))
    print()

    print()
    print()
    print("Ternary search Uniswap V3 swap")
    print()

    start = time.time()
    print("https://etherscan.io/tx/0xe44e28629dde829beba283edae0d54351fa78ec5d3c742c326b839e2841e9fd9")
    path = [
        {
            "chain": "ethereum",
            "variation": "uniswap_v3",
            "fee_tier": "3000",
            "id": "0x6c5f970c5282f0e332e04a9da7a6359fa6c50372",
            "liquidity": "1590547944959088210716",
            "current_price": "408181981782301633885883847",
            "current_tick": "-105373",
            "ticks": {
                -123000: 551819972606031193434,
                -123900: 1038727972353057017282,
                -48300: -1590547944959088210716
            },
            "token_in_decimals": "18",
            "token_in_id": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
            "token_in_symbol": "WETH",
            "token_out_decimals": "18",
            "token_out_id": "0xb56b726322d4eabc48cc49a0e1c860c657075310",
            "token_out_symbol": "ROOF",
            "zero_for_one": False
        }
    ]
    result = router(path, 200000000000000000)
    print("Execution time:", (time.time() - start) * 1000)
    print("Input amount:", result[1][0])
    print("Output amount:", result[0])
    print("Swaps:", " -> ".join([str(x) for x in result[1]]))
    print()

    start = time.time()
    print("https://etherscan.io/tx/0x829670a934e230bc119d09632ca0dd6cbc6e7b7437ff5276e838215f21c6f079")
    path = [
        {
            "chain": "ethereum",
            "variation": "uniswap_v3",
            "fee_tier": "10000",
            "id": "0x3e87d4c24fb56c52a0dbd089a6219b7086d577d8",
            "liquidity": "6047848101650479583140",
            "current_price": "3116288742245328436113744007",
            "current_tick": "-64718",
            "ticks": {
                -41200: -26457487710184084868,
                -42400: -8288034183305872487,
                -46800: -130959631129938835997,
                -47600: -95655760708550516458,
                -50200: -42976421834989520557,
                -52000: 57262453761747481997,
                -53000: -237374068869056604,
                -54200: -1099511627776,
                -54400: 130959632229450463773,
                -56400: -76784661907919245210,
                -57800: 103242149618103330078,
                -59000: -1762682952954573897,
                -59200: -38909616215582173255,
                -60000: -1093719088735926611331,
                -60200: -424497032431311686601,
                -60600: -75199681152890126256,
                -60800: -745392924070451661766,
                -61400: -2250351563421622595,
                -63400: - 3489961980560960178195
            },
            "token_in_decimals": "18",
            "token_in_id": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
            "token_in_symbol": "WETH",
            "token_out_decimals": "18",
            "token_out_id": "0x3a856d4effa670c54585a5d523e96513e148e95d",
            "token_out_symbol": "TRIAS",
            "zero_for_one": False
        }
    ]
    result = router(path, 11100000000000000)
    print("Execution time:", (time.time() - start) * 1000)
    print("Input amount:", result[1][0])
    print("Output amount:", result[0])
    print("Swaps:", " -> ".join([str(x) for x in result[1]]))
    print()

    start = time.time()
    print("https://etherscan.io/tx/0xe5f4811d52f1c79ac38455f5f892f40763dcb16fff472b7837695a6d55f17e2b")
    path = [
        {
            "chain": "ethereum",
            "variation": "uniswap_v3",
            "fee_tier": "3000",
            "id": "0x6c5f970c5282f0e332e04a9da7a6359fa6c50372",
            "liquidity": "1590547944959088210716",
            "current_price": "418114468039891727909103886",
            "current_tick": "-104892",
            "ticks": {
                -123000: 551819972606031193434,
                -123900: 1038727972353057017282,
                -48300: -1590547944959088210716
            },
            "token_in_decimals": "18",
            "token_in_id": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
            "token_in_symbol": "WETH",
            "token_out_decimals": "18",
            "token_out_id": "0xb56b726322d4eabc48cc49a0e1c860c657075310",
            "token_out_symbol": "ROOF",
            "zero_for_one": True
        }
    ]
    result = router(path, 5173752409264797647058)
    print("Execution time:", (time.time() - start) * 1000)
    print("Input amount:", result[1][0])
    print("Output amount:", result[0])
    print("Swaps:", " -> ".join([str(x) for x in result[1]]))
    print()


    ticks_swap_1 = [
        {
          "liquidityNet": "-15427312422210888",
          "tickIdx": "0"
        },
        {
          "liquidityNet": "19639133380239729751",
          "tickIdx": "-108210"
        },
        {
          "liquidityNet": "38522975611217713593",
          "tickIdx": "-125370"
        },
        {
          "liquidityNet": "15427312422210888",
          "tickIdx": "-138160"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-230270"
        },
        {
          "liquidityNet": "-345404955522929921",
          "tickIdx": "23030"
        },
        {
          "liquidityNet": "-1735090298278099150725",
          "tickIdx": "-23040"
        },
        {
          "liquidityNet": "1735090298278099150725",
          "tickIdx": "-23990"
        },
        {
          "liquidityNet": "-38522975611217713593",
          "tickIdx": "-32960"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-52990"
        },
        {
          "liquidityNet": "-901815989351508048601",
          "tickIdx": "-55220"
        },
        {
          "liquidityNet": "-26344788194240398",
          "tickIdx": "-57040"
        },
        {
          "liquidityNet": "-19639133380239729751",
          "tickIdx": "-57050"
        },
        {
          "liquidityNet": "-95221219172218982024",
          "tickIdx": "-59920"
        },
        {
          "liquidityNet": "-52454461514548841698",
          "tickIdx": "-59930"
        },
        {
          "liquidityNet": "-3060982994281482915",
          "tickIdx": "-61660"
        },
        {
          "liquidityNet": "-28543843641725691823",
          "tickIdx": "-62150"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-65310"
        },
        {
          "liquidityNet": "-85791629093958050231438",
          "tickIdx": "-65510"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-66200"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-66210"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-66690"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-66860"
        },
        {
          "liquidityNet": "5812623076452005012674",
          "tickIdx": "67260"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-67460"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-67470"
        },
        {
          "liquidityNet": "-31069591674002309369",
          "tickIdx": "-69030"
        },
        {
          "liquidityNet": "-5812623076452005012674",
          "tickIdx": "69080"
        },
        {
          "liquidityNet": "-233753195961766146869",
          "tickIdx": "-69080"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-69090"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-69350"
        },
        {
          "liquidityNet": "-1561916945735078601228",
          "tickIdx": "-70030"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-70180"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-70220"
        },
        {
          "liquidityNet": "-842413537574643511354",
          "tickIdx": "-70280"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-70720"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-70900"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-70930"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-71100"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-71130"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-71260"
        },
        {
          "liquidityNet": "-54327818436882521719",
          "tickIdx": "-71310"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-71700"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-71710"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-71770"
        },
        {
          "liquidityNet": "-90908420880439063769",
          "tickIdx": "-71900"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-72010"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-72080"
        },
        {
          "liquidityNet": "-13250157521107237367",
          "tickIdx": "-72230"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-72320"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-72350"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-72400"
        },
        {
          "liquidityNet": "-10723394709895687487861",
          "tickIdx": "-72420"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-72440"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-72450"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-72480"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-72570"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-72590"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-72630"
        },
        {
          "liquidityNet": "-462471743147216374264",
          "tickIdx": "-72980"
        },
        {
          "liquidityNet": "-11329005191753462022",
          "tickIdx": "-73070"
        },
        {
          "liquidityNet": "-48421535329086459987",
          "tickIdx": "-73080"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-73100"
        },
        {
          "liquidityNet": "-49269055039717802161515",
          "tickIdx": "-73130"
        },
        {
          "liquidityNet": "-1243030124270936031724",
          "tickIdx": "-73140"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-73160"
        },
        {
          "liquidityNet": "-1221692890193222463227",
          "tickIdx": "-73200"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-73220"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-73320"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-73380"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-73400"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-73420"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-73510"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-73530"
        },
        {
          "liquidityNet": "-408626000666039875345",
          "tickIdx": "-73600"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-73610"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-73660"
        },
        {
          "liquidityNet": "-10340004776136493730",
          "tickIdx": "-73690"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-73730"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-73740"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-73760"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-73770"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-73780"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-73820"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-73840"
        },
        {
          "liquidityNet": "-18604917648380992736",
          "tickIdx": "-73890"
        },
        {
          "liquidityNet": "-109409336063256334715",
          "tickIdx": "-73900"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-73910"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-73970"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-73980"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-74020"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-74050"
        },
        {
          "liquidityNet": "-1821818911634435235931",
          "tickIdx": "-74060"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-74070"
        },
        {
          "liquidityNet": "-1794000309260246025181",
          "tickIdx": "-74090"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-74100"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-74130"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-74160"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-74170"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-74180"
        }
    ]
    
    formated_ticks_swap_1 = dict()
    for tick in ticks_swap_1:
        formated_ticks_swap_1[int(tick["tickIdx"])] = int(tick["liquidityNet"])
    
    start = time.time()
    print("https://etherscan.io/tx/0x20bfd24d9290cb947b23d8520f5aa85ac732b31aaf9371509e80a6a9da5b767c")
    path = [
        {
            "chain": "ethereum",
            "variation": "uniswap_v3",
            "fee_tier": "500",
            "id": "0x60594a405d53811d3bc4766596efd80fd545a270",
            "liquidity": "4766288142532819653166406",
            "current_price": "1394234512488199283911371265",
            "current_tick": "-80804",
            "ticks": formated_ticks_swap_1,
            "token_in_decimals": "18",
            "token_in_id": "",
            "token_in_symbol": "",
            "token_out_decimals": "18",
            "token_out_id": "",
            "token_out_symbol": "",
            "zero_for_one": False
        }
    ]
    result = router(path, 103640625000000000000, True)
    print("Execution time:", (time.time() - start) * 1000)
    print("Input amount:", result[1][0])
    print("Output amount:", result[0])
    print("Swaps:", " -> ".join([str(x) for x in result[1]]))
    print()

    ticks_swap_2 = [
        {
          "liquidityNet": "-91372846132699078323",
          "tickIdx": "10"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "110"
        },
        {
          "liquidityNet": "-210302746405159257",
          "tickIdx": "-197250"
        },
        {
          "liquidityNet": "-864769579966161",
          "tickIdx": "-197280"
        },
        {
          "liquidityNet": "210302746405159257",
          "tickIdx": "-197300"
        },
        {
          "liquidityNet": "864769579966161",
          "tickIdx": "-197400"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-242310"
        },
        {
          "liquidityNet": "-1059987331357746",
          "tickIdx": "-244130"
        },
        {
          "liquidityNet": "1059987331357746",
          "tickIdx": "-246360"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-246370"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-253300"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-265680"
        },
        {
          "liquidityNet": "-77428904874239",
          "tickIdx": "-269390"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-271940"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-272250"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-272270"
        },
        {
          "liquidityNet": "-263314386910981068",
          "tickIdx": "-273050"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-273780"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-273930"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-274090"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-274440"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-274500"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-274920"
        },
        {
          "liquidityNet": "-2493370967581957",
          "tickIdx": "-274930"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-275160"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-275260"
        },
        {
          "liquidityNet": "-693542921054872",
          "tickIdx": "-275270"
        },
        {
          "liquidityNet": "-1780494319989135",
          "tickIdx": "-275280"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-275340"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-275360"
        },
        {
          "liquidityNet": "-1220461423983350212",
          "tickIdx": "-275370"
        },
        {
          "liquidityNet": "-387630136685118674",
          "tickIdx": "-275420"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-275460"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-275500"
        },
        {
          "liquidityNet": "-221505407847434",
          "tickIdx": "-275660"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-275740"
        },
        {
          "liquidityNet": "-589666095829075",
          "tickIdx": "-275810"
        },
        {
          "liquidityNet": "-442003876884273363",
          "tickIdx": "-275830"
        },
        {
          "liquidityNet": "-1487364390802174",
          "tickIdx": "-275840"
        },
        {
          "liquidityNet": "-1987693149483922690",
          "tickIdx": "-275900"
        },
        {
          "liquidityNet": "-5162113950898695",
          "tickIdx": "-275930"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-275940"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-275990"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-276000"
        },
        {
          "liquidityNet": "-39586817307743201",
          "tickIdx": "-276020"
        },
        {
          "liquidityNet": "-27314220186754053",
          "tickIdx": "-276030"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-276040"
        },
        {
          "liquidityNet": "-6124834742829561",
          "tickIdx": "-276080"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-276090"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-276100"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-276110"
        },
        {
          "liquidityNet": "-10492826430304607183",
          "tickIdx": "-276120"
        },
        {
          "liquidityNet": "-718616468958192384",
          "tickIdx": "-276130"
        },
        {
          "liquidityNet": "-1626130288345334",
          "tickIdx": "-276140"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-276150"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-276170"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-276180"
        },
        {
          "liquidityNet": "-225729476917120864",
          "tickIdx": "-276190"
        },
        {
          "liquidityNet": "-1453343076275070825",
          "tickIdx": "-276200"
        },
        {
          "liquidityNet": "-87012672997226927",
          "tickIdx": "-276210"
        },
        {
          "liquidityNet": "-31059448124092511843",
          "tickIdx": "-276220"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "276220"
        },
        {
          "liquidityNet": "-5243776793301468574",
          "tickIdx": "-276230"
        },
        {
          "liquidityNet": "-36736625637384941780",
          "tickIdx": "-276240"
        },
        {
          "liquidityNet": "-7041374915027279990",
          "tickIdx": "-276250"
        },
        {
          "liquidityNet": "-21912377675702866196",
          "tickIdx": "-276260"
        },
        {
          "liquidityNet": "-1028038196713573875143",
          "tickIdx": "-276270"
        },
        {
          "liquidityNet": "-4649800345154973883",
          "tickIdx": "-276280"
        },
        {
          "liquidityNet": "-468046203163295278296",
          "tickIdx": "-276290"
        },
        {
          "liquidityNet": "-5572876028670098255488",
          "tickIdx": "-276300"
        },
        {
          "liquidityNet": "-134157172381640371378659",
          "tickIdx": "-276310"
        },
        {
          "liquidityNet": "113712021195883153494491",
          "tickIdx": "-276320"
        },
        {
          "liquidityNet": "26612982872593168777846",
          "tickIdx": "-276330"
        },
        {
          "liquidityNet": "452330481344075989388",
          "tickIdx": "-276340"
        },
        {
          "liquidityNet": "146728526063723830805",
          "tickIdx": "-276350"
        },
        {
          "liquidityNet": "114289608043242923951",
          "tickIdx": "-276360"
        },
        {
          "liquidityNet": "242110011955366268199",
          "tickIdx": "-276370"
        },
        {
          "liquidityNet": "13827618319340613424",
          "tickIdx": "-276380"
        },
        {
          "liquidityNet": "146751228064306293",
          "tickIdx": "-276390"
        },
        {
          "liquidityNet": "4766057597498106687",
          "tickIdx": "-276400"
        },
        {
          "liquidityNet": "739539542583876538",
          "tickIdx": "-276410"
        },
        {
          "liquidityNet": "31096056818405985451",
          "tickIdx": "-276420"
        },
        {
          "liquidityNet": "822293460629229506",
          "tickIdx": "-276430"
        },
        {
          "liquidityNet": "2658781473677180887",
          "tickIdx": "-276440"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-276450"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-276460"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-276470"
        },
        {
          "liquidityNet": "27018711057390021",
          "tickIdx": "-276480"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-276490"
        },
        {
          "liquidityNet": "657837074129038036",
          "tickIdx": "-276500"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-276510"
        },
        {
          "liquidityNet": "10492826430304607183",
          "tickIdx": "-276520"
        },
        {
          "liquidityNet": "62578785893871291",
          "tickIdx": "-276530"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-276540"
        },
        {
          "liquidityNet": "6124834742829561",
          "tickIdx": "-276560"
        },
        {
          "liquidityNet": "3664917804772426",
          "tickIdx": "-276570"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-276580"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-276590"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-276600"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "-276610"
        }
    ]
    formated_ticks_swap_2 = dict()
    for tick in ticks_swap_2:
        formated_ticks_swap_2[int(tick["tickIdx"])] = int(tick["liquidityNet"])

    path = [
        {
            "chain": "ethereum",
            "variation": "uniswap_v3",
            "fee_tier": "500",
            "id": "0x6c6bc977e13df9b0de53b251522280bb72383700",
            "liquidity": "141350142051284148706870",
            "current_price": "79258725870858806182852",
            "current_tick": "-276317",
            "ticks": formated_ticks_swap_2,
            "token_in_decimals": "18",
            "token_in_id": "",
            "token_in_symbol": "",
            "token_out_decimals": "6",
            "token_out_id": "",
            "token_out_symbol": "",
            "zero_for_one": True
        }
    ]
    result = router(path, 334089294273728152411560, True)
    print("Execution time:", (time.time() - start) * 1000)
    print("Input amount:", result[1][0])
    print("Output amount:", result[0])
    print("Swaps:", " -> ".join([str(x) for x in result[1]]))
    print()

    ticks_swap_3 = [
        {
          "liquidityNet": "100",
          "tickIdx": "100"
        },
        {
          "liquidityNet": "451082180332",
          "tickIdx": "108340"
        },
        {
          "liquidityNet": "2273693713",
          "tickIdx": "108360"
        },
        {
          "liquidityNet": "44739669244",
          "tickIdx": "108390"
        },
        {
          "liquidityNet": "446606638860",
          "tickIdx": "108460"
        },
        {
          "liquidityNet": "-100",
          "tickIdx": "110"
        },
        {
          "liquidityNet": "50623993764010",
          "tickIdx": "115140"
        },
        {
          "liquidityNet": "39265540233720",
          "tickIdx": "150780"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "150890"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "153470"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "153840"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "157130"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "159170"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "159180"
        },
        {
          "liquidityNet": "29522482240085",
          "tickIdx": "161190"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "164760"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "166300"
        },
        {
          "liquidityNet": "306893119116861",
          "tickIdx": "168120"
        },
        {
          "liquidityNet": "14465742111463",
          "tickIdx": "169160"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "170120"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "170410"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "172180"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "175050"
        },
        {
          "liquidityNet": "4921975053928",
          "tickIdx": "175460"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "176120"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "176330"
        },
        {
          "liquidityNet": "944343167794936",
          "tickIdx": "177280"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "177290"
        },
        {
          "liquidityNet": "33451854543234",
          "tickIdx": "178070"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "179830"
        },
        {
          "liquidityNet": "1945470872801668",
          "tickIdx": "180160"
        },
        {
          "liquidityNet": "21844719377840292",
          "tickIdx": "180740"
        },
        {
          "liquidityNet": "4243967061865503",
          "tickIdx": "182110"
        },
        {
          "liquidityNet": "1131670065466301",
          "tickIdx": "182200"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "182390"
        },
        {
          "liquidityNet": "600765393344935",
          "tickIdx": "182450"
        },
        {
          "liquidityNet": "7886769913169",
          "tickIdx": "182510"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "182980"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "183260"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "183290"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "183710"
        },
        {
          "liquidityNet": "109678179589544",
          "tickIdx": "184170"
        },
        {
          "liquidityNet": "40617221865583",
          "tickIdx": "184190"
        },
        {
          "liquidityNet": "17409290878",
          "tickIdx": "184200"
        },
        {
          "liquidityNet": "229571938869626",
          "tickIdx": "184210"
        },
        {
          "liquidityNet": "2330211525502976",
          "tickIdx": "184220"
        },
        {
          "liquidityNet": "3522052025368368",
          "tickIdx": "184440"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "184790"
        },
        {
          "liquidityNet": "41695412458200",
          "tickIdx": "185000"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "185180"
        },
        {
          "liquidityNet": "18175967839443",
          "tickIdx": "185260"
        },
        {
          "liquidityNet": "645894603582312",
          "tickIdx": "185270"
        },
        {
          "liquidityNet": "328005897522005",
          "tickIdx": "185380"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "185390"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "185580"
        },
        {
          "liquidityNet": "169929386233181569",
          "tickIdx": "185720"
        },
        {
          "liquidityNet": "79333854514189",
          "tickIdx": "185810"
        },
        {
          "liquidityNet": "18195637022287213",
          "tickIdx": "185890"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "185960"
        },
        {
          "liquidityNet": "3663233155068602",
          "tickIdx": "186200"
        },
        {
          "liquidityNet": "4056664485988",
          "tickIdx": "186350"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "186410"
        },
        {
          "liquidityNet": "6943945564211028",
          "tickIdx": "186440"
        },
        {
          "liquidityNet": "1160463822411561",
          "tickIdx": "186450"
        },
        {
          "liquidityNet": "329742910333494",
          "tickIdx": "186520"
        },
        {
          "liquidityNet": "19042975966677684",
          "tickIdx": "186730"
        },
        {
          "liquidityNet": "5385967211497235",
          "tickIdx": "187000"
        },
        {
          "liquidityNet": "974235476092981",
          "tickIdx": "187080"
        },
        {
          "liquidityNet": "1590325559201059",
          "tickIdx": "187090"
        },
        {
          "liquidityNet": "97415128084463983",
          "tickIdx": "187310"
        },
        {
          "liquidityNet": "37638126479778693",
          "tickIdx": "187740"
        },
        {
          "liquidityNet": "228738222608205",
          "tickIdx": "187780"
        },
        {
          "liquidityNet": "75916542054248",
          "tickIdx": "187920"
        },
        {
          "liquidityNet": "576894205557474",
          "tickIdx": "188220"
        },
        {
          "liquidityNet": "351995416057517",
          "tickIdx": "188500"
        },
        {
          "liquidityNet": "576556301834907",
          "tickIdx": "188520"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "188800"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "188950"
        },
        {
          "liquidityNet": "118976753284174",
          "tickIdx": "189060"
        },
        {
          "liquidityNet": "22570565946057",
          "tickIdx": "189240"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "189300"
        },
        {
          "liquidityNet": "1116958942568668",
          "tickIdx": "189310"
        },
        {
          "liquidityNet": "24617220867216594",
          "tickIdx": "189320"
        },
        {
          "liquidityNet": "2771560181789014",
          "tickIdx": "189340"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "189480"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "189490"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "189790"
        },
        {
          "liquidityNet": "845606680697538",
          "tickIdx": "189980"
        },
        {
          "liquidityNet": "97760202983867",
          "tickIdx": "190190"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "190320"
        },
        {
          "liquidityNet": "3023468183420544",
          "tickIdx": "190370"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "190380"
        },
        {
          "liquidityNet": "370913814107552",
          "tickIdx": "190490"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "190570"
        },
        {
          "liquidityNet": "31011492802880837",
          "tickIdx": "190650"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "190660"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "190740"
        },
        {
          "liquidityNet": "134596206659304",
          "tickIdx": "190750"
        },
        {
          "liquidityNet": "0",
          "tickIdx": "190920"
        },
        {
          "liquidityNet": "25292570502",
          "tickIdx": "190950"
        }
    ]
    formated_ticks_swap_3 = dict()
    for tick in ticks_swap_3:
        formated_ticks_swap_3[int(tick["tickIdx"])] = int(tick["liquidityNet"])

    path = [
        {
            "chain": "ethereum",
            "variation": "uniswap_v3",
            "fee_tier": "500",
            "id": "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
            "liquidity": "14151942239244037361",
            "current_price": "1397049851824581359551408321000000",
            "current_tick": "195560",
            "ticks": formated_ticks_swap_3,
            "token_in_decimals": "6",
            "token_in_id": "",
            "token_in_symbol": "",
            "token_out_decimals": "18",
            "token_out_id": "",
            "token_out_symbol": "",
            "zero_for_one": True
        }
    ]
    result = router(path, 334177803068, True)
    print("Execution time:", (time.time() - start) * 1000)
    print("Input amount:", result[1][0])
    print("Output amount:", result[0])
    print("Swaps:", " -> ".join([str(x) for x in result[1]]))
    print()

    path = [
        {
            "chain": "ethereum",
            "variation": "uniswap_v3",
            "fee_tier": "500",
            "id": "0x60594a405d53811d3bc4766596efd80fd545a270",
            "liquidity": "4766288142532819653166406",
            "current_price": "1394234512488199283911371265",
            "current_tick": "-80804",
            "ticks": formated_ticks_swap_1,
            "token_in_decimals": "18",
            "token_in_id": "",
            "token_in_symbol": "",
            "token_out_decimals": "18",
            "token_out_id": "",
            "token_out_symbol": "",
            "zero_for_one": False
        },
        {
            "chain": "ethereum",
            "variation": "uniswap_v3",
            "fee_tier": "500",
            "id": "0x6c6bc977e13df9b0de53b251522280bb72383700",
            "liquidity": "141350142051284148706870",
            "current_price": "79258725870858806182852",
            "current_tick": "-276317",
            "ticks": formated_ticks_swap_2,
            "token_in_decimals": "18",
            "token_in_id": "",
            "token_in_symbol": "",
            "token_out_decimals": "6",
            "token_out_id": "",
            "token_out_symbol": "",
            "zero_for_one": True
        },
        {
            "chain": "ethereum",
            "variation": "uniswap_v3",
            "fee_tier": "500",
            "id": "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
            "liquidity": "14151942239244037361",
            "current_price": "1397049851824581359551408321000000",
            "current_tick": "195560",
            "ticks": formated_ticks_swap_3,
            "token_in_decimals": "6",
            "token_in_id": "",
            "token_in_symbol": "",
            "token_out_decimals": "18",
            "token_out_id": "",
            "token_out_symbol": "",
            "zero_for_one": True
        }
    ]
    result = router(path, 103640625000000000000, True)
    print("Execution time:", (time.time() - start) * 1000)
    print("Input amount:", result[1][0])
    print("Output amount:", result[0])
    print("Profit:", result[0]-result[1][0])
    print("Swaps:", " -> ".join([str(x) for x in result[1]]))
    print()

    path = [
        {
            "chain": "ethereum",
            "variation": "uniswap_v3",
            "fee_tier": "500",
            "id": "0x60594a405d53811d3bc4766596efd80fd545a270",
            "liquidity": "4766288142532819653166406",
            "current_price": "1394234512488199283911371265",
            "current_tick": "-80804",
            "ticks": formated_ticks_swap_1,
            "token_in_decimals": "18",
            "token_in_id": "",
            "token_in_symbol": "",
            "token_out_decimals": "18",
            "token_out_id": "",
            "token_out_symbol": "",
            "zero_for_one": False
        },
        {
            "chain": "ethereum",
            "variation": "uniswap_v3",
            "fee_tier": "500",
            "id": "0x6c6bc977e13df9b0de53b251522280bb72383700",
            "liquidity": "141350142051284148706870",
            "current_price": "79258725870858806182852",
            "current_tick": "-276317",
            "ticks": formated_ticks_swap_2,
            "token_in_decimals": "18",
            "token_in_id": "",
            "token_in_symbol": "",
            "token_out_decimals": "6",
            "token_out_id": "",
            "token_out_symbol": "",
            "zero_for_one": True
        },
        {
            "chain": "ethereum",
            "variation": "uniswap_v3",
            "fee_tier": "500",
            "id": "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
            "liquidity": "14151942239244037361",
            "current_price": "1397049851824581359551408321000000",
            "current_tick": "195560",
            "ticks": formated_ticks_swap_3,
            "token_in_decimals": "6",
            "token_in_id": "",
            "token_in_symbol": "",
            "token_out_decimals": "18",
            "token_out_id": "",
            "token_out_symbol": "",
            "zero_for_one": True
        }
    ]
    result = ternary_search(path, 100000000000000)
    print("Execution time:", (time.time() - start) * 1000)
    print("Input amount:", result[0])
    print("Output amount:", result[2][-1])
    print("Profit:", result[1])
    print("Swaps:", " -> ".join([str(x) for x in result[2]]))
    print()

   