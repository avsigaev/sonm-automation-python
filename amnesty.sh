set_sonmcli() {
	if [ -f "./sonmcli" ]; then
		sonmcli="./sonmcli"
	else
		sonmcli="sonmcli"
	fi
}

amnesty() {
	local blacklist=$($sonmcli blacklist list  --out=json | jq '.addresses' | tr -d '"[],\0' | grep -v null) 
	if [ -z "$blacklist" ]; then
			echo 'Blacklist is clean.'
		else
			echo 'Blacklisted suppliers:' 
			for i in "${blacklist[*]}"; do  					
					echo $i && echo ""
				done
			for i in $blacklist; do  					
					$sonmcli blacklist remove $i && echo $i "succesfully released from blacklist."
				done			
	fi
}

set_sonmcli
amnesty