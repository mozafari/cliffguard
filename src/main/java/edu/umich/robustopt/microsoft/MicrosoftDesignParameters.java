package edu.umich.robustopt.microsoft;

import edu.umich.robustopt.dbd.DesignParameters;

public class MicrosoftDesignParameters extends DesignParameters {
	
	public final MicrosoftDesignAddMode designAddMode;
	public final MicrosoftDesignKeepMode designKeepMode;
	public final MicrosoftDesignOnlineOption designOnlineOption;

	public MicrosoftDesignParameters(MicrosoftDesignAddMode designAddMode,
			MicrosoftDesignKeepMode designKeepMode, MicrosoftDesignOnlineOption onlineOption) {
		super();
		this.designAddMode = designAddMode;
		this.designKeepMode = designKeepMode;
		this.designOnlineOption = onlineOption;
	}

	@Override
	public String toString() {
		return "designAddMode=" + designAddMode + ", designKeepMode=" + designKeepMode + ", designOnlineOption=" + designOnlineOption;
	}
	
	
}
